package io.nekohasekai.sagernet.bg

import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.database.ProxyEntity
import io.nekohasekai.sagernet.utils.DiagnosticLog
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

internal object AutoProfileScoreStore {

    enum class Event {
        CANDIDATE,
        PRE_SWITCH,
        POST_CONNECT,
        PERIODIC,
    }

    private data class ProfileKey(val mode: Int, val profileId: Long)

    private data class ProfileScore(
        val score: Int = INITIAL_SCORE,
        val successes: Int = 0,
        val strongSuccesses: Int = 0,
        val updatedAt: Long = 0L,
        val vip: Boolean = false,
    )

    private const val VERSION_PREFIX = "v2|"
    private const val INITIAL_SCORE = 25
    private const val VIP_ENTER_SCORE = 85
    private const val VIP_EXIT_SCORE = 75
    private const val VIP_MIN_SUCCESSES = 8
    private const val VIP_MIN_STRONG_SUCCESSES = 3
    private const val MAX_RECORDS = 50_000
    private const val PERSIST_DELAY_MS = 5_000L

    private val lock = Any()
    private val records = LinkedHashMap<ProfileKey, ProfileScore>()
    private val persistScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var loaded = false
    private var dirty = false
    private var persistJob: Job? = null

    fun recordSuccess(
        profile: ProxyEntity,
        mode: Int,
        event: Event,
        afterFailure: Boolean = false,
    ) {
        val delta = when (event) {
            Event.CANDIDATE -> 1
            Event.PRE_SWITCH -> 3
            Event.POST_CONNECT -> 6
            Event.PERIODIC -> if (afterFailure) 4 else 12
        }
        update(profile, mode, "${event.name.lowercase()}-success") { current, now ->
            val successes = (current.successes + 1).coerceAtMost(100_000)
            val strongSuccesses = if (event == Event.POST_CONNECT || event == Event.PERIODIC) {
                (current.strongSuccesses + 1).coerceAtMost(100_000)
            } else {
                current.strongSuccesses
            }
            val score = (current.score + delta).coerceIn(0, 100)
            current.copy(
                score = score,
                successes = successes,
                strongSuccesses = strongSuccesses,
                updatedAt = now,
                vip = if (current.vip) {
                    score >= VIP_EXIT_SCORE
                } else {
                    score >= VIP_ENTER_SCORE &&
                            successes >= VIP_MIN_SUCCESSES &&
                            strongSuccesses >= VIP_MIN_STRONG_SUCCESSES
                },
            )
        }
    }

    fun recordFailure(profile: ProxyEntity, mode: Int, event: Event) {
        val delta = when (event) {
            Event.CANDIDATE -> -2
            Event.PRE_SWITCH -> -5
            Event.POST_CONNECT -> -8
            Event.PERIODIC -> -10
        }
        update(profile, mode, "${event.name.lowercase()}-failure") { current, now ->
            val score = (current.score + delta).coerceIn(0, 100)
            current.copy(
                score = score,
                updatedAt = now,
                vip = current.vip && score >= VIP_EXIT_SCORE,
            )
        }
    }

    fun recordManualSwitch(profile: ProxyEntity, mode: Int) {
        update(profile, mode, "manual-switch") { current, now ->
            val score = (current.score - 12).coerceIn(0, 100)
            current.copy(
                score = score,
                updatedAt = now,
                vip = current.vip && score >= VIP_EXIT_SCORE,
            )
        }
        schedulePersist(immediate = true)
    }

    fun orderForScan(profiles: List<ProxyEntity>, mode: Int): List<ProxyEntity> {
        if (profiles.size < 2) return profiles
        ensureLoaded()
        val safeMode = sanitizeMode(mode)
        val scores = synchronized(lock) {
            profiles.associate { profile ->
                profile.id to (records[ProfileKey(safeMode, profile.id)] ?: ProfileScore())
            }
        }
        val vipCount = profiles.count { scores[it.id]?.vip == true }
        if (vipCount == 0) return profiles

        DiagnosticLog.event(
            "auto-vpn",
            "vip-cache-priority mode=$safeMode vip=$vipCount total=${profiles.size} " +
                    "threshold=$VIP_ENTER_SCORE minSuccesses=$VIP_MIN_SUCCESSES",
        )
        return profiles.withIndex()
            .sortedWith(
                compareBy<IndexedValue<ProxyEntity>> { if (scores[it.value.id]?.vip == true) 0 else 1 }
                    .thenByDescending { if (scores[it.value.id]?.vip == true) scores[it.value.id]?.score ?: 0 else 0 }
                    .thenBy { it.index },
            )
            .map { it.value }
    }

    fun score(profile: ProxyEntity?, mode: Int): Int {
        if (profile == null || profile.id <= 0L) return INITIAL_SCORE
        ensureLoaded()
        return synchronized(lock) {
            records[ProfileKey(sanitizeMode(mode), profile.id)]?.score ?: INITIAL_SCORE
        }
    }

    fun isVip(profile: ProxyEntity?, mode: Int): Boolean {
        if (profile == null || profile.id <= 0L) return false
        ensureLoaded()
        return synchronized(lock) {
            records[ProfileKey(sanitizeMode(mode), profile.id)]?.vip == true
        }
    }

    fun flush() {
        ensureLoaded()
        synchronized(lock) {
            persistJob?.cancel()
            persistJob = null
            persistLocked()
        }
    }

    private fun update(
        profile: ProxyEntity,
        mode: Int,
        reason: String,
        transform: (ProfileScore, Long) -> ProfileScore,
    ) {
        if (profile.id <= 0L) return
        ensureLoaded()
        val safeMode = sanitizeMode(mode)
        val now = System.currentTimeMillis()
        val old: ProfileScore
        val updated: ProfileScore
        synchronized(lock) {
            val key = ProfileKey(safeMode, profile.id)
            old = records[key] ?: ProfileScore()
            updated = transform(old, now)
            records[key] = updated
            dirty = true
            trimIfNeededLocked()
        }
        DiagnosticLog.event(
            "auto-vpn",
            "profile-score mode=$safeMode profile=${profile.id} reason=$reason " +
                    "${old.score}->${updated.score} vip=${updated.vip}",
        )
        schedulePersist()
    }

    private fun ensureLoaded() {
        if (loaded) return
        synchronized(lock) {
            if (loaded) return
            records.clear()
            val encoded = DataStore.autoVpnProfileScoreStats.orEmpty()
            if (encoded.startsWith(VERSION_PREFIX)) {
                encoded.removePrefix(VERSION_PREFIX)
                    .split(';')
                    .asSequence()
                    .filter { it.isNotBlank() }
                    .forEach { item ->
                        val fields = item.split(',')
                        if (fields.size != 7) return@forEach
                        val mode = fields[0].toIntOrNull()?.let(::sanitizeMode) ?: return@forEach
                        val id = fields[1].toLongOrNull()?.takeIf { it > 0L } ?: return@forEach
                        val score = fields[2].toIntOrNull()?.coerceIn(0, 100) ?: return@forEach
                        val successes = fields[3].toIntOrNull()?.coerceAtLeast(0) ?: return@forEach
                        val strongSuccesses = fields[4].toIntOrNull()?.coerceAtLeast(0) ?: return@forEach
                        val updatedAt = fields[5].toLongOrNull()?.coerceAtLeast(0L) ?: return@forEach
                        val storedVip = fields[6] == "1"
                        val eligible = score >= VIP_ENTER_SCORE &&
                                successes >= VIP_MIN_SUCCESSES &&
                                strongSuccesses >= VIP_MIN_STRONG_SUCCESSES
                        records[ProfileKey(mode, id)] = ProfileScore(
                            score = score,
                            successes = successes,
                            strongSuccesses = strongSuccesses,
                            updatedAt = updatedAt,
                            vip = if (storedVip) score >= VIP_EXIT_SCORE else eligible,
                        )
                    }
            }
            trimIfNeededLocked()
            loaded = true
        }
    }

    private fun schedulePersist(immediate: Boolean = false) {
        synchronized(lock) {
            if (!dirty) return
            if (immediate) {
                persistJob?.cancel()
            } else if (persistJob?.isActive == true) {
                return
            }
            persistJob = persistScope.launch {
                if (!immediate) delay(PERSIST_DELAY_MS)
                synchronized(lock) {
                    persistJob = null
                    persistLocked()
                }
            }
        }
    }

    private fun persistLocked() {
        if (!dirty) return
        val encoded = records.entries
            .sortedByDescending { it.value.updatedAt }
            .take(MAX_RECORDS)
            .joinToString(";") { (key, score) ->
                listOf(
                    key.mode,
                    key.profileId,
                    score.score,
                    score.successes,
                    score.strongSuccesses,
                    score.updatedAt,
                    if (score.vip) 1 else 0,
                ).joinToString(",")
            }
        DataStore.autoVpnProfileScoreStats = VERSION_PREFIX + encoded
        dirty = false
    }

    private fun trimIfNeededLocked() {
        if (records.size <= MAX_RECORDS) return
        val keep = records.entries
            .sortedByDescending { it.value.updatedAt }
            .take(MAX_RECORDS)
            .mapTo(HashSet()) { it.key }
        records.keys.retainAll(keep)
    }

    private fun sanitizeMode(mode: Int): Int = when (mode) {
        AutoVpnManager.MODE_NON_RU -> AutoVpnManager.MODE_NON_RU
        AutoVpnManager.MODE_GEMINI -> AutoVpnManager.MODE_GEMINI
        else -> AutoVpnManager.MODE_ANY
    }
}
