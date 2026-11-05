/******************************************************************************
 *                                                                            *
 * Copyright (C) 2026                                                         *
 *                                                                            *
 * This program is free software: you can redistribute it and/or modify        *
 * it under the terms of the GNU General Public License as published by        *
 * the Free Software Foundation, either version 3 of the License, or           *
 *  (at your option) any later version.                                        *
 *                                                                            *
 ******************************************************************************/

package io.nekohasekai.sagernet.bg

import android.content.Intent
import android.database.sqlite.SQLiteDatabaseLockedException
import android.net.Network
import android.net.NetworkCapabilities
import android.widget.Toast
import androidx.core.app.NotificationManagerCompat
import io.nekohasekai.sagernet.Action
import io.nekohasekai.sagernet.GroupOrder
import io.nekohasekai.sagernet.Key
import io.nekohasekai.sagernet.R
import io.nekohasekai.sagernet.RouteMode
import io.nekohasekai.sagernet.SagerNet
import io.nekohasekai.sagernet.aidl.TrafficStats
import io.nekohasekai.sagernet.TunImplementation
import io.nekohasekai.sagernet.bg.test.V2RayTestInstance
import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.database.GroupManager
import io.nekohasekai.sagernet.database.ProfileManager
import io.nekohasekai.sagernet.database.ProxyEntity
import io.nekohasekai.sagernet.database.ProxyListItem
import io.nekohasekai.sagernet.database.SagerDatabase
import io.nekohasekai.sagernet.ktx.Logs
import io.nekohasekai.sagernet.ktx.app
import io.nekohasekai.sagernet.ktx.applyDefaultValues
import io.nekohasekai.sagernet.ktx.onMainDispatcher
import io.nekohasekai.sagernet.ktx.onIoDispatcher
import io.nekohasekai.sagernet.ktx.readableMessage
import io.nekohasekai.sagernet.ktx.runOnDefaultDispatcher
import io.nekohasekai.sagernet.ktx.runOnMainDispatcher
import io.nekohasekai.sagernet.utils.DiagnosticLog
import io.nekohasekai.sagernet.utils.DefaultNetworkListener
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException
import java.net.IDN
import java.net.HttpURLConnection
import java.net.URL
import java.util.LinkedHashSet
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

object AutoVpnManager {

    const val AUTO_PROFILE_ID = -1L
    const val AUTO_NON_RU_PROFILE_ID = -2L
    const val AUTO_GEMINI_PROFILE_ID = -3L

    const val MODE_ANY = 0
    const val MODE_NON_RU = 1
    const val MODE_GEMINI = 2

    private const val CURRENT_VERIFY_ATTEMPTS = 3
    private const val CURRENT_VERIFY_RETRY_DELAY_MS = 2000L
    private const val RECENT_WORKING_TTL_MS = 30L * 60L * 1000L
    private const val AUTO_SCAN_PAGE_SIZE = 200
    private const val AUTO_SCAN_NEXT_CYCLE_DELAY_MS = 1000L
    private const val DIRECT_TEST_VPN_SETTLE_DELAY_MS = 2000L
    private const val DIRECT_TEST_VPN_STOP_TIMEOUT_MS = 15_000L
    private const val DIRECT_TEST_VPN_STOP_POLL_DELAY_MS = 100L
    private const val AUTO_SEARCH_NOTIFICATION_ID = 4
    private const val DIRECT_NETWORK_CHECK_TIMEOUT_MS = 4_000L
    private const val DIRECT_NETWORK_CONNECT_TIMEOUT_MS = 3_000
    private const val DIRECT_NETWORK_READ_TIMEOUT_MS = 3_000
    private const val DIRECT_NETWORK_CHECK_URL = "https://ya.ru/"
    private const val EXIT_COUNTRY_CONFIRMATION_DELAY_MS = 45_000L
    private const val NETWORK_STATUS_CONNECT_TIMEOUT_MS = 3_000
    private const val NETWORK_STATUS_READ_TIMEOUT_MS = 3_000
    private const val NETWORK_STATUS_NETWORK_TIMEOUT_MS = 5_000L
    private const val NETWORK_STATUS_GOOGLE_URL = "https://www.google.com/generate_204"
    private const val NETWORK_STATUS_YANDEX_URL = "https://ya.ru/"
    private const val TRAFFIC_SAMPLE_INTERVAL_MS = 10_000L
    private const val TRAFFIC_SUSPICIOUS_INTERVALS = 2
    private const val TRAFFIC_OUTGOING_MIN_BYTES = 1024L
    private const val TRAFFIC_INCOMING_MAX_BYTES = 256L
    private const val TRAFFIC_CONFIRM_DELAY_MS = 5_000L
    private const val TRAFFIC_DIAGNOSTIC_COOLDOWN_MS = 60_000L
    private const val SERVICE_AVAILABILITY_REFRESH_INTERVAL_MS = 2L * 60L * 1000L
    private const val SERVICE_AVAILABILITY_STALE_CHECK_MS = 30_000L
    private const val SERVICE_AVAILABILITY_CONFIRM_DELAY_MS = 5_000L
    private const val SCORE_NETWORK_STATUS_CACHE_MS = 15_000L
    private const val MAX_GEMINI_REJECTED_KEYS = 5000
    private const val PROFILE_RESULT_DB_RETRY_COUNT = 4
    private const val PROFILE_RESULT_DB_RETRY_DELAY_MS = 250L
    private const val RUSSIA_FLAG = "\uD83C\uDDF7\uD83C\uDDFA"
    private const val RUSSIA_CYRILLIC_TLD = "\u0440\u0444"
    private val POST_CONNECT_VERIFY_DELAYS_MS = longArrayOf(3000L, 5000L, 5000L)

    private data class CandidateResult(
        val profile: ProxyEntity,
        val ping: Int,
    )

    private data class CandidateOutcome(
        val result: CandidateResult?,
        val elapsedMillis: Long,
        val error: Throwable?,
    )

    private data class CachedWorking(
        val ping: Int,
        val at: Long,
    )

    private data class ActiveCheckResult(
        val bad: Boolean,
        val ping: Int? = null,
        val exitCountryRejected: Boolean = false,
    )

    private data class PendingExitCountryConfirmation(
        val profile: ProxyEntity,
        val mode: Int,
        var countryCode: String?,
        val source: String,
        val scoreEvent: AutoProfileScoreStore.Event,
    )

    private data class ServiceAvailabilitySnapshot(
        val checkedAt: Long,
        val results: List<ServiceAvailabilityProbe.Result>,
    )

    private data class AutoProgressSnapshot(
        val generation: Long,
        val running: Boolean,
        val total: Int,
        val finished: Int,
        val working: Int,
    ) {
        fun encode(): String = listOf(
            generation,
            if (running) 1 else 0,
            total,
            finished,
            working,
        ).joinToString("|")
    }

    private class AutoTestThrottle(
        private val quickFailureLimit: Int,
        private val quickFailureMillis: Int,
        private val quickFailurePauseMillis: Long,
        private val scheduledBreakAfterCount: Int,
        private val scheduledBreakMillis: Long,
    ) {
        private var quickFailureCounter = 0
        private var quickFailurePauseUntil = 0L
        private var scheduledBreakCounter = 0
        private var scheduledBreakUntil = 0L

        suspend fun waitForPause() {
            while (true) {
                val now = System.currentTimeMillis()
                val wait = maxOf(quickFailurePauseUntil, scheduledBreakUntil) - now
                if (wait <= 0L) return
                delay(wait.coerceAtMost(250L))
            }
        }

        fun record(outcome: CandidateOutcome) {
            val error = outcome.error
            if (outcome.result != null) {
                quickFailureCounter = 0
            } else if (error != null) {
                val quickNetworkFailure =
                    outcome.elapsedMillis <= quickFailureMillis &&
                            ProfileTestRunner.isLikelyTransientNetworkFailure(error)
                if (quickNetworkFailure) {
                    quickFailureCounter += 1
                    if (quickFailureCounter >= quickFailureLimit) {
                        quickFailureCounter = 0
                        quickFailurePauseUntil = maxOf(
                            quickFailurePauseUntil,
                            System.currentTimeMillis() + quickFailurePauseMillis,
                        )
                    }
                } else {
                    quickFailureCounter = 0
                }
            }

            if (scheduledBreakAfterCount > 0 && scheduledBreakMillis > 0L) {
                scheduledBreakCounter += 1
                if (scheduledBreakCounter >= scheduledBreakAfterCount) {
                    scheduledBreakCounter = 0
                    scheduledBreakUntil = maxOf(
                        scheduledBreakUntil,
                        System.currentTimeMillis() + scheduledBreakMillis,
                    )
                }
            }
        }
    }

    private data class Ipv4Cidr(
        val network: Long,
        val mask: Long,
    ) {
        fun contains(ip: Long): Boolean = ip and mask == network
    }

    private val switchMutex = Mutex()
    private val scoreNetworkCheckMutex = Mutex()
    private val exitCountryCheckMutex = Mutex()
    private val exitCountryConfirmationLock = Any()
    private val failedUntil = ConcurrentHashMap<Long, Long>()
    private val workingCache = ConcurrentHashMap<Long, CachedWorking>()
    private val activeTrafficSpeedBaselines = mutableMapOf<Long, Long>()
    private val activeTrafficSpeedLock = Any()
    private val workingPools = mutableMapOf<Int, List<Long>>()
    private val hydratedReserveModes = mutableSetOf<Int>()
    private val manualCycleIds = mutableMapOf<Int, LinkedHashSet<Long>>()
    private val scannedIds = mutableMapOf<Int, LinkedHashSet<Long>>()
    private val exitCountryRejectedWriteLock = Any()
    private var exitCountryConfirmationJob: Job? = null
    private var pendingExitCountryConfirmation: PendingExitCountryConfirmation? = null
    @Volatile
    private var cachedExitCountryRejectedKeys: Set<String>? = null
    @Volatile
    private var cachedExitCountryRejectedRevision = Long.MIN_VALUE
    private val listeners = LinkedHashSet<Listener>()
    private val progressLock = Any()
    private val russianTokenRegex = Regex(
        """(^|[^a-z0-9\u0430-\u044f])(?:ru|rus|russia|russian|\u0440\u043e\u0441\u0441\u0438\u044f|\u0440\u043e\u0441\u0441\u0438\u0439\u0441\u043a\u0438\u0439|\u043c\u043e\u0441\u043a\u0432\u0430|moscow|msk|spb|\u0440\u0444)(?=$|[^a-z0-9\u0430-\u044f])""",
        RegexOption.IGNORE_CASE,
    )
    private val isoCountryCodes by lazy { Locale.getISOCountries().toSet() }
    private val knownCountryNameRegex by lazy {
        val names = (Locale.getISOCountries().flatMap { code ->
            val country = Locale.Builder().setRegion(code).build()
            listOf(
                country.getDisplayCountry(Locale.ENGLISH),
                country.getDisplayCountry(Locale.forLanguageTag("ru")),
                country.getDisplayCountry(Locale.getDefault()),
            )
        } + listOf("USA", "UAE", "США", "ОАЭ", "Великобритания"))
            .asSequence()
            .map { it.trim().lowercase(Locale.ROOT).replace('ё', 'е') }
            .filter { it.length >= 3 }
            .distinct()
            .sortedByDescending { it.length }
            .joinToString("|") { Regex.escape(it) }
        Regex("(?<![\\p{L}\\p{N}])(?:$names)(?![\\p{L}\\p{N}])")
    }
    private val russianIpv4Cidrs by lazy {
        listOf(
            "2.60.0.0/14",
            "5.8.0.0/13",
            "5.16.0.0/12",
            "31.173.0.0/16",
            "37.9.0.0/16",
            "37.140.0.0/16",
            "46.17.40.0/21",
            "46.38.48.0/20",
            "46.61.0.0/16",
            "77.88.0.0/18",
            "79.104.0.0/13",
            "80.68.240.0/20",
            "81.19.64.0/20",
            "83.220.160.0/19",
            "84.201.128.0/18",
            "87.240.128.0/18",
            "89.108.64.0/18",
            "90.156.128.0/17",
            "93.158.128.0/18",
            "95.163.0.0/16",
            "109.95.0.0/16",
            "109.120.128.0/17",
            "178.154.128.0/17",
            "185.32.56.0/22",
            "185.71.76.0/22",
            "213.180.192.0/19",
        ).mapNotNull(::parseCidr)
    }

    private var monitorJob: Job? = null
    private var immediateVerifyJob: Job? = null
    private val immediateVerifyGeneration = AtomicLong(0L)
    private val activeProfileCheckGeneration = AtomicLong(0L)
    private var autoScanJob: Job? = null
    private var trafficMonitorJob: Job? = null
    private var trafficDiagnosticJob: Job? = null
    private val serviceAvailabilityLock = Any()
    private var serviceAvailabilityJob: Job? = null
    private var serviceAvailabilityAfterConnectJob: Job? = null
    private var googleAvailabilityConfirmationJob: Job? = null
    private var geminiAvailabilityConfirmationJob: Job? = null
    private val siteAvailabilityConfirmationJobs = mutableMapOf<ServiceAvailabilityProbe.Service, Job>()
    @Volatile
    private var serviceAvailabilityProfileId = -1L
    @Volatile
    private var serviceAvailabilityMode = -1
    @Volatile
    private var serviceAvailabilityCheckedAt = 0L
    @Volatile
    private var serviceAvailabilityResults = emptyList<ServiceAvailabilityProbe.Result>()
    private var lastTrafficDiagnosticAt = 0L
    private val networkStatusMutex = Mutex()
    @Volatile
    private var networkStatusStringId = R.string.auto_vpn_network_checking
    private var failures = 0
    @Volatile
    private var scoreNetworkAvailable = true
    @Volatile
    private var scoreNetworkCheckedAt = 0L
    private var lastSwitchAt = 0L
    private var lastAutoProgressNotifyAt = 0L
    private var autoProgressGeneration = 0L
    private var lastCheckedPingMillis: Int? = null
    private var lastCheckedPingFailed = false
    @Volatile
    private var lastDecisionReason: String? = null
    @Volatile
    private var pendingAutoServiceTransition = false
    @Volatile
    private var notificationStatus: String? = null
    @Volatile
    private var notificationDetailStatus: String? = null
    @Volatile
    private var notificationBriefStatus: String? = null
    @Volatile
    private var activeUrlTester: (() -> Int)? = null
    @Volatile
    private var activeTrafficSampler: (suspend () -> TrafficStats?)? = null
    @Volatile
    var autoRunning = false
        private set

    @Volatile
    var autoTotal = 0
        private set

    @Volatile
    var autoFinished = 0
        private set

    @Volatile
    private var autoDisplayedFinished = 0

    @Volatile
    var autoWorking = 0
        private set

    @Volatile
    var testResultsResetGeneration = 0
        private set

    interface Listener {
        fun onAutoVpnStateChanged()
    }

    fun addListener(listener: Listener) {
        synchronized(listeners) {
            listeners += listener
        }
        listener.onAutoVpnStateChanged()
    }

    fun removeListener(listener: Listener) {
        synchronized(listeners) {
            listeners -= listener
        }
    }

    fun hasVisibleState(): Boolean {
        syncVisibleStateFromStore()
        return DataStore.autoVpnEnabled && (autoRunning || notificationStatus != null)
    }

    fun isAutoRunning(): Boolean {
        syncVisibleStateFromStore()
        return autoRunning
    }

    fun hasProgressState(): Boolean {
        syncVisibleStateFromStore()
        return DataStore.autoVpnEnabled && (autoRunning || autoTotal > 0)
    }

    fun progressText(): String {
        syncVisibleStateFromStore()
        return app.getString(R.string.connection_test_progress, autoDisplayedFinished, autoTotal)
    }

    fun progressPercent(): Int {
        syncVisibleStateFromStore()
        val total = autoTotal
        return if (total > 0) {
            ((autoDisplayedFinished.toDouble() / total.toDouble()) * 100).toInt().coerceIn(0, 100)
        } else {
            0
        }
    }

    fun reserveStatus(mode: Int = DataStore.autoVpnMode): String {
        syncVisibleStateFromStore()
        val count = reserveCount(mode)
        return app.getString(R.string.auto_vpn_candidates_count, count, reserveTarget())
    }

    fun appWorkingText(mode: Int = DataStore.autoVpnMode): String {
        syncVisibleStateFromStore()
        return reserveStatus(mode)
    }

    fun cardStatusText(mode: Int = DataStore.autoVpnMode): String {
        syncVisibleStateFromStore()
        val parts = ArrayList<String>()
        parts += notificationDetailStatus
            ?: notificationStatus
            ?: app.getString(R.string.auto_vpn_notification_running, modeName(mode))
        if (!autoRunning) lastDecisionReason?.let { parts += it }
        lastPingText()?.let { parts += it }
        candidateCountText(mode)?.let { parts += it }
        lastExitCountryText()?.let { parts += it }
        return parts.joinToString(" | ")
    }

    fun serviceAvailabilityText(mode: Int = DataStore.autoVpnMode): String? {
        if (mode != DataStore.autoVpnMode) return null
        val profileId = DataStore.startedProfile
        val connectionActive = DataStore.autoVpnEnabled && SagerNet.started && profileId > 0L
        val localSnapshot = if (connectionActive) synchronized(serviceAvailabilityLock) {
            if (profileId == serviceAvailabilityProfileId && mode == serviceAvailabilityMode &&
                serviceAvailabilityResults.isNotEmpty()
            ) {
                ServiceAvailabilitySnapshot(
                    checkedAt = serviceAvailabilityCheckedAt,
                    results = serviceAvailabilityResults.toList(),
                )
            } else null
        } else null
        val persistedSnapshot = if (connectionActive) readServiceAvailabilitySnapshot(profileId, mode) else null
        val snapshot = when {
            localSnapshot == null -> persistedSnapshot
            persistedSnapshot == null -> localSnapshot
            persistedSnapshot.checkedAt >= localSnapshot.checkedAt -> persistedSnapshot
            else -> localSnapshot
        }
        val results = snapshot?.results ?: ServiceAvailabilityProbe.notCheckedResults()
        val serviceLines = results.joinToString("\n") { result ->
            val statusRes = when (result.state) {
                ServiceAvailabilityProbe.State.NOT_CHECKED -> R.string.auto_vpn_service_not_checked
                ServiceAvailabilityProbe.State.CHECKING -> R.string.auto_vpn_service_checking
                ServiceAvailabilityProbe.State.AVAILABLE -> R.string.auto_vpn_service_available
                ServiceAvailabilityProbe.State.UNAVAILABLE -> R.string.auto_vpn_service_unavailable
                ServiceAvailabilityProbe.State.NO_RESPONSE -> R.string.auto_vpn_service_no_response
                ServiceAvailabilityProbe.State.RESTRICTED -> R.string.auto_vpn_service_restricted
                ServiceAvailabilityProbe.State.UNKNOWN -> R.string.auto_vpn_service_unknown
                ServiceAvailabilityProbe.State.NO_TUNNEL_ROUTE -> R.string.auto_vpn_service_no_tunnel_route
                ServiceAvailabilityProbe.State.FORBIDDEN -> R.string.auto_vpn_service_forbidden
                ServiceAvailabilityProbe.State.CHALLENGE -> R.string.auto_vpn_service_challenge
            }
            app.getString(
                R.string.auto_vpn_service_status_line,
                app.getString(result.service.nameRes),
                app.getString(statusRes),
            )
        }
        return "${networkStatusText()}\n$serviceLines"
    }

    private fun readServiceAvailabilitySnapshot(profileId: Long, mode: Int): ServiceAvailabilitySnapshot? {
        val encodedSnapshot = DataStore.autoVpnServiceAvailabilitySnapshot
        if (encodedSnapshot.isBlank()) return null
        return runCatching {
            val json = JSONObject(encodedSnapshot)
            if (json.optLong("profileId", -1L) != profileId || json.optInt("mode", -1) != mode) {
                return null
            }
            val encodedResults = json.optJSONArray("results") ?: return null
            val byService = LinkedHashMap<ServiceAvailabilityProbe.Service, ServiceAvailabilityProbe.Result>()
            for (index in 0 until encodedResults.length()) {
                val item = encodedResults.optJSONObject(index) ?: continue
                val service = ServiceAvailabilityProbe.Service.values()
                    .firstOrNull { it.name == item.optString("service") } ?: continue
                val state = ServiceAvailabilityProbe.State.values()
                    .firstOrNull { it.name == item.optString("state") } ?: continue
                byService[service] = ServiceAvailabilityProbe.Result(
                    service = service,
                    state = state,
                    responseCode = if (item.isNull("responseCode")) null else item.optInt("responseCode"),
                    elapsedMillis = item.optLong("elapsedMillis"),
                    finalLocation = if (item.isNull("finalLocation")) null else item.optString("finalLocation"),
                )
            }
            if (ServiceAvailabilityProbe.Service.values().any { it !in byService }) return null
            ServiceAvailabilitySnapshot(
                checkedAt = json.optLong("checkedAt"),
                results = ServiceAvailabilityProbe.Service.values().map { byService.getValue(it) },
            )
        }.onFailure {
            DiagnosticLog.warn("auto-vpn-services", "stored-availability-snapshot-read-failed", it)
        }.getOrNull()
    }

    private fun persistServiceAvailabilitySnapshotLocked() {
        runCatching {
            val encodedResults = JSONArray().apply {
                serviceAvailabilityResults.forEach { result ->
                    put(
                        JSONObject()
                            .put("service", result.service.name)
                            .put("state", result.state.name)
                            .put("responseCode", result.responseCode ?: JSONObject.NULL)
                            .put("elapsedMillis", result.elapsedMillis)
                            .put("finalLocation", result.finalLocation ?: JSONObject.NULL),
                    )
                }
            }
            DataStore.autoVpnServiceAvailabilitySnapshot = JSONObject()
                .put("profileId", serviceAvailabilityProfileId)
                .put("mode", serviceAvailabilityMode)
                .put("checkedAt", serviceAvailabilityCheckedAt)
                .put("results", encodedResults)
                .toString()
        }.onFailure {
            DiagnosticLog.error("auto-vpn-services", "stored-availability-snapshot-write-failed", it)
        }
    }

    private fun resetServiceAvailabilityForConnection(profileId: Long, mode: Int) {
        clearPendingExitCountryConfirmation()
        synchronized(serviceAvailabilityLock) {
            serviceAvailabilityJob?.cancel()
            serviceAvailabilityJob = null
            serviceAvailabilityAfterConnectJob?.cancel()
            serviceAvailabilityAfterConnectJob = null
            googleAvailabilityConfirmationJob?.cancel()
            googleAvailabilityConfirmationJob = null
            geminiAvailabilityConfirmationJob?.cancel()
            geminiAvailabilityConfirmationJob = null
            siteAvailabilityConfirmationJobs.values.forEach { it.cancel() }
            siteAvailabilityConfirmationJobs.clear()
            serviceAvailabilityProfileId = profileId
            serviceAvailabilityMode = sanitizeMode(mode)
            serviceAvailabilityCheckedAt = 0L
            serviceAvailabilityResults = emptyList()
            persistServiceAvailabilitySnapshotLocked()
        }
    }

    private fun clearServiceAvailability() {
        clearPendingExitCountryConfirmation()
        synchronized(serviceAvailabilityLock) {
            serviceAvailabilityJob?.cancel()
            serviceAvailabilityJob = null
            serviceAvailabilityAfterConnectJob?.cancel()
            serviceAvailabilityAfterConnectJob = null
            googleAvailabilityConfirmationJob?.cancel()
            googleAvailabilityConfirmationJob = null
            geminiAvailabilityConfirmationJob?.cancel()
            geminiAvailabilityConfirmationJob = null
            siteAvailabilityConfirmationJobs.values.forEach { it.cancel() }
            siteAvailabilityConfirmationJobs.clear()
            serviceAvailabilityProfileId = -1L
            serviceAvailabilityMode = -1
            serviceAvailabilityCheckedAt = 0L
            serviceAvailabilityResults = emptyList()
            persistServiceAvailabilitySnapshotLocked()
        }
    }

    private fun scheduleServiceAvailabilityCheck(profileId: Long, mode: Int, force: Boolean = false) {
        val safeMode = sanitizeMode(mode)
        if (profileId <= 0L || !isSameStartedAutoProfile(profileId, safeMode)) return
        synchronized(serviceAvailabilityLock) {
            val persistedSnapshot = readServiceAvailabilitySnapshot(profileId, safeMode)
            val localSameTarget = serviceAvailabilityProfileId == profileId && serviceAvailabilityMode == safeMode
            val sameTarget = localSameTarget || persistedSnapshot != null
            val lastCheckedAt = maxOf(serviceAvailabilityCheckedAt, persistedSnapshot?.checkedAt ?: 0L)
            val checkedRecently = lastCheckedAt > 0L &&
                    System.currentTimeMillis() - lastCheckedAt < SERVICE_AVAILABILITY_REFRESH_INTERVAL_MS
            if (localSameTarget && serviceAvailabilityJob?.isActive == true) {
                if (checkedRecently && !force) return
                serviceAvailabilityJob?.cancel()
                serviceAvailabilityJob = null
            }
            val hasCompletedResults = serviceAvailabilityResults.any {
                it.state != ServiceAvailabilityProbe.State.CHECKING
            } || persistedSnapshot?.results?.any {
                it.state != ServiceAvailabilityProbe.State.CHECKING
            } == true
            if (sameTarget && (hasCompletedResults || persistedSnapshot != null) && checkedRecently && !force) return

            googleAvailabilityConfirmationJob?.cancel()
            googleAvailabilityConfirmationJob = null
            geminiAvailabilityConfirmationJob?.cancel()
            geminiAvailabilityConfirmationJob = null
            siteAvailabilityConfirmationJobs.values.forEach { it.cancel() }
            siteAvailabilityConfirmationJobs.clear()
            serviceAvailabilityProfileId = profileId
            serviceAvailabilityMode = safeMode
            serviceAvailabilityCheckedAt = System.currentTimeMillis()
            serviceAvailabilityResults = ServiceAvailabilityProbe.checkingResults()
            persistServiceAvailabilitySnapshotLocked()
            serviceAvailabilityJob = runOnDefaultDispatcher {
                val startedAt = System.currentTimeMillis()
                try {
                    DiagnosticLog.event(
                        "auto-vpn-services",
                        "availability-check-start profile=$profileId mode=$safeMode force=$force",
                    )
                    val geminiFilterEnabled = safeMode == MODE_GEMINI ||
                            ServiceAvailabilityProbe.Service.GEMINI.name in DataStore.autoVpnSiteSwitchFilters
                    val results = ServiceAvailabilityProbe.checkAll(renderGeminiPage = geminiFilterEnabled)
                    if (!isSameStartedAutoProfile(profileId, safeMode)) return@runOnDefaultDispatcher
                    val accepted = synchronized(serviceAvailabilityLock) {
                        if (serviceAvailabilityProfileId != profileId || serviceAvailabilityMode != safeMode) {
                            false
                        } else {
                            serviceAvailabilityResults = results
                            serviceAvailabilityCheckedAt = System.currentTimeMillis()
                            serviceAvailabilityJob = null
                            persistServiceAvailabilitySnapshotLocked()
                            true
                        }
                    }
                    if (!accepted) return@runOnDefaultDispatcher
                    DiagnosticLog.event(
                        "auto-vpn-services",
                        "availability-check-finished profile=$profileId mode=$safeMode elapsedMs=${System.currentTimeMillis() - startedAt} " +
                                results.joinToString(",") {
                                    "${it.service.name}:${it.state}:http=${it.responseCode ?: "none"}" +
                                            (if (it.service == ServiceAvailabilityProbe.Service.GEMINI) ":location=${it.finalLocation.orEmpty()}" else "") +
                                            ":${it.elapsedMillis}ms"
                                },
                    )
                    notifyAutoStateChanged()
                    if (areAllSitesUnresponsive(results)) {
                        val directInternetAvailable = hasDirectInternet()
                        if (!isSameStartedAutoProfile(profileId, safeMode)) return@runOnDefaultDispatcher
                        DiagnosticLog.critical(
                            "auto-vpn-services",
                            "all-sites-unresponsive profile=$profileId mode=$safeMode directInternet=$directInternetAvailable",
                        )
                        if (directInternetAvailable) {
                            confirmActiveProfileFailure(profileId, safeMode, "all-sites-unresponsive")
                        } else {
                            deferForMissingDirectNetwork(profileId, safeMode, trafficTriggered = false)
                        }
                        return@runOnDefaultDispatcher
                    }
                    results.firstOrNull { it.service == ServiceAvailabilityProbe.Service.GOOGLE }
                        ?.takeIf { isSiteSwitchFilterEnabled(it.service, safeMode) }
                        ?.takeIf {
                            it.state == ServiceAvailabilityProbe.State.UNAVAILABLE ||
                                    it.state == ServiceAvailabilityProbe.State.NO_RESPONSE
                        }
                        ?.let { scheduleGoogleAvailabilityConfirmation(profileId, safeMode, it) }
                    results.firstOrNull { it.service == ServiceAvailabilityProbe.Service.GEMINI }
                        ?.takeIf { isSiteSwitchFilterEnabled(it.service, safeMode) }
                        ?.takeIf { it.requiresGeminiConfirmation() }
                        ?.let { scheduleGeminiAvailabilityConfirmation(profileId, safeMode, it) }
                    results.filter {
                        it.service != ServiceAvailabilityProbe.Service.GOOGLE &&
                                it.service != ServiceAvailabilityProbe.Service.GEMINI &&
                                isSiteSwitchFilterEnabled(it.service, safeMode) &&
                                it.isSiteUnavailableForSwitch()
                    }.forEach { scheduleSiteAvailabilityConfirmation(profileId, safeMode, it) }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    DiagnosticLog.error("auto-vpn-services", "availability-check-failed profile=$profileId mode=$safeMode", e)
                    synchronized(serviceAvailabilityLock) {
                        if (serviceAvailabilityProfileId == profileId && serviceAvailabilityMode == safeMode) {
                            serviceAvailabilityResults = ServiceAvailabilityProbe.checkingResults().map {
                                it.copy(state = ServiceAvailabilityProbe.State.UNKNOWN)
                            }
                            serviceAvailabilityCheckedAt = System.currentTimeMillis()
                            serviceAvailabilityJob = null
                            persistServiceAvailabilitySnapshotLocked()
                        }
                    }
                    notifyAutoStateChanged()
                }
            }
        }
        notifyAutoStateChanged()
    }

    fun networkStatusText(): String = app.getString(networkStatusStringId)

    private fun areAllSitesUnresponsive(results: List<ServiceAvailabilityProbe.Result>): Boolean {
        return results.size == ServiceAvailabilityProbe.Service.values().size && results.all {
            it.responseCode == null && it.state in setOf(
                ServiceAvailabilityProbe.State.NO_RESPONSE,
                ServiceAvailabilityProbe.State.UNKNOWN,
            )
        }
    }

    private fun isSiteSwitchFilterEnabled(service: ServiceAvailabilityProbe.Service, mode: Int): Boolean =
        service == ServiceAvailabilityProbe.Service.GEMINI && mode == MODE_GEMINI ||
                service.name in DataStore.autoVpnSiteSwitchFilters

    private fun ServiceAvailabilityProbe.Result.isSiteUnavailableForSwitch(): Boolean = when (state) {
        ServiceAvailabilityProbe.State.AVAILABLE,
        ServiceAvailabilityProbe.State.CHECKING,
        ServiceAvailabilityProbe.State.NOT_CHECKED,
        ServiceAvailabilityProbe.State.NO_TUNNEL_ROUTE -> false
        else -> true
    }

    private fun scheduleSiteAvailabilityConfirmation(
        profileId: Long,
        mode: Int,
        first: ServiceAvailabilityProbe.Result,
    ) {
        val service = first.service
        synchronized(serviceAvailabilityLock) {
            if (siteAvailabilityConfirmationJobs[service]?.isActive == true) return
            siteAvailabilityConfirmationJobs[service] = runOnDefaultDispatcher {
                try {
                    delay(SERVICE_AVAILABILITY_CONFIRM_DELAY_MS)
                    if (!isSameStartedAutoProfile(profileId, mode) || !isSiteSwitchFilterEnabled(service, mode)) {
                        return@runOnDefaultDispatcher
                    }
                    val second = ServiceAvailabilityProbe.checkService(service)
                    if (!isSameStartedAutoProfile(profileId, mode)) return@runOnDefaultDispatcher
                    val accepted = synchronized(serviceAvailabilityLock) {
                        if (serviceAvailabilityProfileId != profileId || serviceAvailabilityMode != mode) {
                            false
                        } else {
                            serviceAvailabilityResults = serviceAvailabilityResults.map {
                                if (it.service == service) second else it
                            }
                            serviceAvailabilityCheckedAt = System.currentTimeMillis()
                            persistServiceAvailabilitySnapshotLocked()
                            true
                        }
                    }
                    if (!accepted) return@runOnDefaultDispatcher
                    DiagnosticLog.event(
                        "auto-vpn-services",
                        "site-confirmation profile=$profileId mode=$mode service=${service.name} " +
                                "first=${first.state}:http=${first.responseCode ?: "none"} " +
                                "second=${second.state}:http=${second.responseCode ?: "none"}",
                    )
                    notifyAutoStateChanged()
                    if (!second.isSiteUnavailableForSwitch()) return@runOnDefaultDispatcher
                    if (!hasDirectInternet()) {
                        deferForMissingDirectNetwork(profileId, mode, trafficTriggered = false)
                    } else if (isSiteSwitchFilterEnabled(service, mode)) {
                        synchronized(serviceAvailabilityLock) {
                            siteAvailabilityConfirmationJobs.remove(service)
                        }
                        confirmActiveProfileFailure(profileId, mode, "site-${service.name.lowercase()}-${second.state.name.lowercase()}")
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    DiagnosticLog.error(
                        "auto-vpn-services",
                        "site-confirmation-failed profile=$profileId mode=$mode service=${service.name}",
                        e,
                    )
                } finally {
                    val currentJob = currentCoroutineContext()[Job]
                    synchronized(serviceAvailabilityLock) {
                        if (siteAvailabilityConfirmationJobs[service] === currentJob) {
                            siteAvailabilityConfirmationJobs.remove(service)
                        }
                    }
                }
            }
        }
    }

    fun refreshServiceAvailabilityOnAppResume() {
        val profileId = DataStore.startedProfile
        val mode = sanitizeMode(DataStore.autoVpnMode)
        if (!DataStore.autoVpnEnabled || !SagerNet.started || profileId <= 0L) return
        scheduleServiceAvailabilityCheck(profileId, mode)
    }

    private fun scheduleGoogleAvailabilityConfirmation(
        profileId: Long,
        mode: Int,
        first: ServiceAvailabilityProbe.Result,
    ) {
        synchronized(serviceAvailabilityLock) {
            if (googleAvailabilityConfirmationJob?.isActive == true) return
            googleAvailabilityConfirmationJob = runOnDefaultDispatcher {
                try {
                    delay(SERVICE_AVAILABILITY_CONFIRM_DELAY_MS)
                    if (!isSameStartedAutoProfile(profileId, mode) ||
                        !isSiteSwitchFilterEnabled(ServiceAvailabilityProbe.Service.GOOGLE, mode)
                    ) return@runOnDefaultDispatcher
                    val second = ServiceAvailabilityProbe.checkGoogle()
                    if (!isSameStartedAutoProfile(profileId, mode)) return@runOnDefaultDispatcher
                    val accepted = synchronized(serviceAvailabilityLock) {
                        if (serviceAvailabilityProfileId != profileId || serviceAvailabilityMode != mode) {
                            false
                        } else {
                            serviceAvailabilityResults = serviceAvailabilityResults.map {
                                if (it.service == ServiceAvailabilityProbe.Service.GOOGLE) second else it
                            }
                            serviceAvailabilityCheckedAt = System.currentTimeMillis()
                            persistServiceAvailabilitySnapshotLocked()
                            true
                        }
                    }
                    if (!accepted) return@runOnDefaultDispatcher
                    DiagnosticLog.event(
                        "auto-vpn-services",
                        "google-confirmation profile=$profileId mode=$mode " +
                                "first=${first.state}:http=${first.responseCode ?: "none"} " +
                                "second=${second.state}:http=${second.responseCode ?: "none"}",
                    )
                    notifyAutoStateChanged()
                    if (isSiteSwitchFilterEnabled(ServiceAvailabilityProbe.Service.GOOGLE, mode) &&
                        (second.state == ServiceAvailabilityProbe.State.UNAVAILABLE ||
                        second.state == ServiceAvailabilityProbe.State.NO_RESPONSE
                        )
                    ) {
                        if (!hasDirectInternet()) {
                            synchronized(serviceAvailabilityLock) { googleAvailabilityConfirmationJob = null }
                            deferForMissingDirectNetwork(profileId, mode, trafficTriggered = false)
                        } else {
                            synchronized(serviceAvailabilityLock) { googleAvailabilityConfirmationJob = null }
                            confirmActiveProfileFailure(profileId, mode, "google-unavailable")
                        }
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    DiagnosticLog.error("auto-vpn-services", "google-confirmation-failed profile=$profileId mode=$mode", e)
                } finally {
                    val currentJob = currentCoroutineContext()[Job]
                    synchronized(serviceAvailabilityLock) {
                        if (googleAvailabilityConfirmationJob === currentJob) {
                            googleAvailabilityConfirmationJob = null
                        }
                    }
                }
            }
        }
    }

    private fun ServiceAvailabilityProbe.Result.isConfirmedGeminiFailure(): Boolean =
        service == ServiceAvailabilityProbe.Service.GEMINI && when (state) {
            ServiceAvailabilityProbe.State.UNAVAILABLE,
            ServiceAvailabilityProbe.State.RESTRICTED,
            ServiceAvailabilityProbe.State.CHALLENGE,
            ServiceAvailabilityProbe.State.FORBIDDEN -> true
            else -> false
        }

    private fun ServiceAvailabilityProbe.Result.requiresGeminiConfirmation(): Boolean =
        service == ServiceAvailabilityProbe.Service.GEMINI && when (state) {
            ServiceAvailabilityProbe.State.UNKNOWN,
            ServiceAvailabilityProbe.State.NO_RESPONSE -> true
            else -> isConfirmedGeminiFailure()
        }

    private fun ServiceAvailabilityProbe.Result.isInconclusiveGeminiResult(): Boolean =
        service == ServiceAvailabilityProbe.Service.GEMINI && when (state) {
            ServiceAvailabilityProbe.State.UNKNOWN,
            ServiceAvailabilityProbe.State.NO_RESPONSE,
            ServiceAvailabilityProbe.State.NO_TUNNEL_ROUTE -> true
            else -> false
        }

    private fun scheduleGeminiAvailabilityConfirmation(
        profileId: Long,
        mode: Int,
        first: ServiceAvailabilityProbe.Result,
    ) {
        synchronized(serviceAvailabilityLock) {
            if (geminiAvailabilityConfirmationJob?.isActive == true) return
            geminiAvailabilityConfirmationJob = runOnDefaultDispatcher {
                try {
                    delay(SERVICE_AVAILABILITY_CONFIRM_DELAY_MS)
                    if (!isSameStartedAutoProfile(profileId, mode) ||
                        !isSiteSwitchFilterEnabled(ServiceAvailabilityProbe.Service.GEMINI, mode)
                    ) return@runOnDefaultDispatcher
                    val secondProbe = ServiceAvailabilityProbe.checkGemini(renderPage = true)
                    val selectedResult = if (
                        secondProbe.isInconclusiveGeminiResult() && first.isConfirmedGeminiFailure()
                    ) {
                        first
                    } else {
                        secondProbe
                    }
                    if (!isSameStartedAutoProfile(profileId, mode)) return@runOnDefaultDispatcher
                    val accepted = synchronized(serviceAvailabilityLock) {
                        if (serviceAvailabilityProfileId != profileId || serviceAvailabilityMode != mode) {
                            false
                        } else {
                            serviceAvailabilityResults = serviceAvailabilityResults.map {
                                if (it.service == ServiceAvailabilityProbe.Service.GEMINI) selectedResult else it
                            }
                            serviceAvailabilityCheckedAt = System.currentTimeMillis()
                            persistServiceAvailabilitySnapshotLocked()
                            true
                        }
                    }
                    if (!accepted) return@runOnDefaultDispatcher
                    DiagnosticLog.event(
                        "auto-vpn-services",
                        "gemini-confirmation profile=$profileId mode=$mode " +
                                "first=${first.state}:http=${first.responseCode ?: "none"}:location=${first.finalLocation.orEmpty()} " +
                                "second=${secondProbe.state}:http=${secondProbe.responseCode ?: "none"}:location=${secondProbe.finalLocation.orEmpty()} " +
                                "selected=${selectedResult.state}",
                    )
                    notifyAutoStateChanged()
                    if (isSiteSwitchFilterEnabled(ServiceAvailabilityProbe.Service.GEMINI, mode) &&
                        selectedResult.isConfirmedGeminiFailure()
                    ) {
                        if (!hasDirectInternet()) {
                            synchronized(serviceAvailabilityLock) { geminiAvailabilityConfirmationJob = null }
                            deferForMissingDirectNetwork(profileId, mode, trafficTriggered = false)
                        } else {
                            synchronized(serviceAvailabilityLock) { geminiAvailabilityConfirmationJob = null }
                            confirmActiveProfileFailure(profileId, mode, "gemini-${selectedResult.state.name.lowercase()}")
                        }
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    DiagnosticLog.error("auto-vpn-services", "gemini-confirmation-failed profile=$profileId mode=$mode", e)
                } finally {
                    val currentJob = currentCoroutineContext()[Job]
                    synchronized(serviceAvailabilityLock) {
                        if (geminiAvailabilityConfirmationJob === currentJob) {
                            geminiAvailabilityConfirmationJob = null
                        }
                    }
                }
            }
        }
    }

    fun refreshNetworkStatus(trigger: String) {
        networkStatusStringId = R.string.auto_vpn_network_checking
        notifyAutoStateChanged()
        runOnDefaultDispatcher {
            networkStatusMutex.withLock {
                val (googleReachable, yandexReachable) = probeUnderlyingNetwork()
                networkStatusStringId = when {
                    googleReachable && yandexReachable -> R.string.auto_vpn_network_normal
                    !googleReachable && yandexReachable -> R.string.auto_vpn_network_whitelist
                    googleReachable -> R.string.auto_vpn_network_partial
                    else -> R.string.auto_vpn_network_unavailable
                }
                when (networkStatusStringId) {
                    R.string.auto_vpn_network_normal -> updateRussianDomainRule(enabled = true, trigger)
                    R.string.auto_vpn_network_whitelist -> updateRussianDomainRule(enabled = false, trigger)
                }
                DiagnosticLog.event(
                    "auto-vpn",
                    "network-status trigger=$trigger route=underlying google=$googleReachable yandex=$yandexReachable " +
                            "status=${app.getString(networkStatusStringId)}",
                )
                notifyAutoStateChanged()
            }
        }
    }

    private suspend fun updateRussianDomainRule(enabled: Boolean, trigger: String) {
        val russianDomains = setOf("geosite:category-ru", "geosite:tld-ru")
        val rule = SagerDatabase.rulesDao.allRules().firstOrNull { candidate ->
            candidate.outbound == -1L && candidate.ip.isBlank() &&
                    candidate.domains.lineSequence().map { it.trim().lowercase(Locale.ROOT) }
                        .toSet().containsAll(russianDomains)
        } ?: return
        if (rule.enabled == enabled) return

        rule.enabled = enabled
        SagerDatabase.rulesDao.updateRule(rule)
        DiagnosticLog.critical(
            "auto-vpn",
            "ru-domain-route-updated enabled=$enabled trigger=$trigger routeMode=${DataStore.routeMode}",
        )
        if (DataStore.routeMode == RouteMode.RULE && SagerNet.started) {
            onMainDispatcher { SagerNet.reloadService() }
        }
    }

    private suspend fun probeUnderlyingNetwork(): Pair<Boolean, Boolean> {
        val key = Any()
        val network = CompletableDeferred<Network?>()
        var started = false
        return try {
            DefaultNetworkListener.start(key) { available ->
                if (available != null) network.complete(available)
            }
            started = true
            val underlyingNetwork = withTimeoutOrNull(NETWORK_STATUS_NETWORK_TIMEOUT_MS) { network.await() }
                ?: return false to false
            coroutineScope {
                val google = async { isUrlReachable(underlyingNetwork, NETWORK_STATUS_GOOGLE_URL) }
                val yandex = async { isUrlReachable(underlyingNetwork, NETWORK_STATUS_YANDEX_URL) }
                google.await() to yandex.await()
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            DiagnosticLog.error("auto-vpn", "underlying-network-probe-failed", e)
            false to false
        } finally {
            if (started) runCatching { DefaultNetworkListener.stop(key) }
        }
    }

    private suspend fun isUrlReachable(network: Network, address: String): Boolean = onIoDispatcher {
        val connection = runCatching {
            network.openConnection(URL(address)) as? HttpURLConnection
        }.getOrNull() ?: return@onIoDispatcher false
        try {
            connection.instanceFollowRedirects = true
            connection.requestMethod = "GET"
            connection.connectTimeout = NETWORK_STATUS_CONNECT_TIMEOUT_MS
            connection.readTimeout = NETWORK_STATUS_READ_TIMEOUT_MS
            connection.responseCode in 200..399
        } catch (e: CancellationException) {
            throw e
        } catch (_: Exception) {
            false
        } finally {
            connection.disconnect()
        }
    }

    fun onTrafficMonitorSettingChanged() {
        if (!DataStore.autoVpnTrafficMonitorEnabled) {
            stopTrafficMonitoring()
            DiagnosticLog.event("auto-vpn", "traffic-monitor-disabled-by-setting")
        } else if (DataStore.autoVpnEnabled && SagerNet.started && DataStore.startedProfile > 0L) {
            startTrafficMonitor()
            DiagnosticLog.event("auto-vpn", "traffic-monitor-enabled-by-setting")
        }
    }

    fun diagnosticsText(mode: Int = DataStore.autoVpnMode): String {
        syncVisibleStateFromStore()
        val safeMode = sanitizeMode(mode)
        val currentId = DataStore.startedProfile.takeIf { it > 0L } ?: DataStore.selectedProxy
        val current = SagerDatabase.proxyDao.getById(currentId)
        val currentScore = AutoProfileScoreStore.score(current, safeMode)
        val quarantine = quarantineCount(safeMode)
        return buildString {
            appendLine(app.getString(R.string.auto_vpn_diag_mode, modeName(safeMode)))
            appendLine(app.getString(R.string.auto_vpn_diag_current, current?.displayName().orEmpty().ifBlank { "-" }))
            appendLine(app.getString(R.string.auto_vpn_diag_reason, lastDecisionReason ?: current?.let {
                selectionReason(it, safeMode)
            } ?: "-"))
            appendLine(app.getString(R.string.auto_vpn_diag_score, currentScore))
            appendLine("VIP cache: ${if (AutoProfileScoreStore.isVip(current, safeMode)) "yes" else "no"}")
            appendLine(app.getString(R.string.auto_vpn_diag_reserve, reserveCount(safeMode), reserveTarget()))
            lastExitCountryText()?.let { appendLine(it) }
            appendLine(app.getString(R.string.auto_vpn_diag_progress, autoFinished, autoTotal))
            appendLine(app.getString(R.string.auto_vpn_diag_failures, failures, failuresBeforeSwitch()))
            append(app.getString(R.string.auto_vpn_diag_quarantine, quarantine))
        }
    }

    fun lastPingStatusText(): String? {
        return lastPingText()
    }

    fun hasLastPingError(): Boolean {
        return lastCheckedPingFailed
    }

    fun isGeminiModeActive(): Boolean {
        return DataStore.autoVpnEnabled && sanitizeMode(DataStore.autoVpnMode) == MODE_GEMINI
    }

    fun isAutoProfileId(profileId: Long): Boolean {
        return profileId == AUTO_PROFILE_ID ||
                profileId == AUTO_NON_RU_PROFILE_ID ||
                profileId == AUTO_GEMINI_PROFILE_ID
    }

    fun modeForProfileId(profileId: Long): Int {
        return when (profileId) {
            AUTO_NON_RU_PROFILE_ID -> MODE_NON_RU
            AUTO_GEMINI_PROFILE_ID -> MODE_GEMINI
            else -> MODE_ANY
        }
    }

    fun autoListItems(): List<ProxyListItem> {
        return listOf(
            autoListItem(MODE_ANY),
            autoListItem(MODE_NON_RU),
            autoListItem(MODE_GEMINI),
        )
    }

    fun autoListItem(mode: Int): ProxyListItem {
        syncVisibleStateFromStore()
        val safeMode = sanitizeMode(mode)
        val enabled = DataStore.autoVpnEnabled && DataStore.autoVpnMode == safeMode
        val currentId = DataStore.startedProfile.takeIf { it > 0L } ?: DataStore.selectedProxy
        val current = SagerDatabase.proxyDao.getById(currentId)
        val currentName = current?.displayName().orEmpty()
        val status = when {
            enabled && lastCheckedPingMillis != null -> 1
            enabled && lastCheckedPingFailed -> 3
            enabled && current?.status == 1 -> 1
            enabled -> 0
            else -> 0
        }
        val ping = if (enabled) {
            lastCheckedPingMillis ?: current?.ping ?: 0
        } else {
            0
        }
        val address = when {
            enabled && (SagerNet.started || DataStore.startedProfile > 0L) && currentName.isNotBlank() -> {
                app.getString(R.string.auto_vpn_ready_address, currentName)
            }
            enabled && hasDetailedAppStatus() -> ""
            enabled -> app.getString(R.string.auto_vpn_enabled_short)
            else -> app.getString(R.string.auto_vpn_tap_to_start)
        }

        return ProxyListItem(
            id = profileIdForMode(safeMode),
            groupId = 0L,
            type = -1,
            userOrder = Long.MIN_VALUE + safeMode,
            tx = current?.tx ?: 0L,
            rx = current?.rx ?: 0L,
            status = status,
            ping = ping,
            error = null,
            profileNameCache = when (safeMode) {
                MODE_NON_RU -> "Auto no RU"
                MODE_GEMINI -> "Auto Gemini"
                else -> "Auto"
            },
            displayNameCache = app.getString(
                when (safeMode) {
                    MODE_NON_RU -> R.string.auto_vpn_no_ru_name
                    MODE_GEMINI -> R.string.auto_vpn_gemini_name
                    else -> R.string.auto_vpn_any_name
                }
            ),
            displayTypeCache = app.getString(R.string.auto_vpn_type),
            displayAddressCache = address,
        )
    }

    fun modeName(mode: Int = DataStore.autoVpnMode): String {
        return app.getString(
            when (sanitizeMode(mode)) {
                MODE_NON_RU -> R.string.auto_vpn_no_ru_name
                MODE_GEMINI -> R.string.auto_vpn_gemini_name
                else -> R.string.auto_vpn_any_name
            }
        )
    }

    fun notificationTitle(profileName: String): String {
        if (!DataStore.autoVpnEnabled) return profileName
        return profileName.ifBlank { modeName() }
    }

    fun notificationText(defaultText: CharSequence): CharSequence {
        if (!DataStore.autoVpnEnabled) return defaultText
        syncVisibleStateFromStore()
        val briefStatus = notificationBriefStatus
        if (autoRunning || briefStatus == app.getString(R.string.auto_vpn_notification_searching)) {
            return autoProgressNotificationText()
        }
        if (briefStatus == app.getString(R.string.auto_vpn_notification_checking) ||
            briefStatus == app.getString(R.string.auto_vpn_notification_switching)
        ) {
            return notificationDetailStatus ?: notificationStatus ?: briefStatus
        }
        return notificationDetailStatus ?: notificationStatus ?: defaultText.toString()
    }

    fun selectMode(mode: Int = MODE_ANY) {
        val safeMode = sanitizeMode(mode)
        val modeChanged = !DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode
        if (modeChanged) {
            resetAutoSearchState(cancelScan = true)
            clearServiceAvailability()
        }
        DataStore.autoVpnMode = safeMode
        DataStore.autoVpnEnabled = true
        setAutoStatus(safeMode, R.string.auto_vpn_status_selected)
    }

    fun enableAndStart(mode: Int = MODE_ANY) {
        selectMode(mode)
        startSelectedModeFromVpnButton()
    }

    fun startSelectedModeFromVpnButton(onFinishedWithoutServiceStart: (() -> Unit)? = null): Boolean {
        refreshNetworkStatus("auto-test-start")
        val safeMode = sanitizeMode(DataStore.autoVpnMode)
        DataStore.autoVpnMode = safeMode
        DataStore.autoVpnEnabled = true
        if (!isVpnRunningForAuto()) {
            DiagnosticLog.beginAutoSearchSession("manual-vpn-button")
            DiagnosticLog.critical(
                "auto-vpn",
                "start-from-vpn-button mode=$safeMode activeScan=${autoScanJob?.isActive == true} " +
                        "${DiagnosticLog.stateSummary()}"
            )
            setAutoStatus(safeMode, R.string.auto_vpn_status_initial_search)
            autoScanJob?.cancel()
            autoScanJob = null
            DataStore.autoVpnServiceStartRequested = false
            if (!AutoVpnDirectSearchService.startInitialSearch(app, safeMode)) {
                setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
                onFinishedWithoutServiceStart?.invoke()
            }
            return true
        }
        if (autoScanJob?.isActive == true) {
            DiagnosticLog.event("auto-vpn", "start-from-vpn-button-ignored-active-scan mode=$safeMode")
            onFinishedWithoutServiceStart?.let { callback ->
                val job = autoScanJob
                runOnDefaultDispatcher {
                    job?.join()
                    if (!pendingAutoServiceTransition && !SagerNet.started) {
                        callback()
                    }
                }
            }
            return true
        }
        DiagnosticLog.beginAutoSearchSession("manual-vpn-button")
        DiagnosticLog.critical(
            "auto-vpn",
            "start-from-vpn-button mode=$safeMode activeScan=false ${DiagnosticLog.stateSummary()}"
        )
        setAutoStatus(safeMode, R.string.auto_vpn_status_initial_search)
        autoScanJob?.cancel()
        autoScanJob = null

        autoScanJob = runOnDefaultDispatcher {
            try {
                if (!pauseVpnForDirectTests("initial-scan")) {
                    setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
                    return@runOnDefaultDispatcher
                }
                resetAutoSearchState(cancelScan = false)
                DiagnosticLog.critical("auto-vpn", "initial-scan-reset mode=$safeMode ${DiagnosticLog.stateSummary()}")
                ProfileTestRunner.syncFromStore()
                if (!ProfileTestRunner.isRunning()) {
                    ProfileTestRunner.clearState()
                }

                val groupIds = enabledGroupIds()
                if (groupIds.isEmpty()) {
                    clearAutoProgress()
                    setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
                    abandonPendingAutoTransitionIfStopped()
                    return@runOnDefaultDispatcher
                }

                resetAutoCandidateResults(groupIds, safeMode)
                resetAutoProgress(SagerDatabase.proxyDao.countByGroups(groupIds).coerceAtMost(Int.MAX_VALUE.toLong()).toInt())
                setAutoStatus(safeMode, R.string.auto_vpn_status_initial_search)
                DiagnosticLog.critical(
                    "auto-vpn",
                    "initial-scan-start mode=$safeMode groups=${groupIds.size} total=$autoTotal target=${reserveTarget()}"
                )

                val working = scanInitialWorkingProfiles(
                    groupIds = groupIds,
                    excludedIds = automaticExcludedIds(safeMode, emptyList()),
                    mode = safeMode,
                    target = reserveTarget(),
                    countProgress = true,
                )

                if (DataStore.autoVpnEnabled && DataStore.autoVpnMode == safeMode && working.isEmpty()) {
                    DiagnosticLog.critical("auto-vpn", "initial-scan-no-candidate mode=$safeMode checked=$autoFinished total=$autoTotal")
                    setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
                    abandonPendingAutoTransitionIfStopped()
                } else if (DataStore.autoVpnEnabled && DataStore.autoVpnMode == safeMode) {
                    val best = chooseBestCandidate(working, safeMode) ?: return@runOnDefaultDispatcher
                    DiagnosticLog.critical(
                        "auto-vpn",
                        "initial-scan-selected mode=$safeMode working=${working.size} " +
                                ProfileTestRunner.profileDiagnosticSummary(best.profile)
                    )
                    switchMutex.withLock {
                        if (DataStore.autoVpnEnabled && DataStore.autoVpnMode == safeMode) {
                            if (switchToVerified(
                                    initial = best,
                                    mode = safeMode,
                                    excludedIds = automaticExcludedIds(safeMode, emptyList()),
                                    forceTest = true,
                                    startIfStopped = true,
                                    feedback = true,
                                    recordManualCycle = false,
                                    switchReason = "initial-search",
                                )
                            ) {
                                setAutoReadyStatus(safeMode)
                            }
                        }
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                DiagnosticLog.error("auto-vpn", "initial-scan-error mode=$safeMode", e)
                Logs.w(e)
                if (DataStore.autoVpnEnabled && DataStore.autoVpnMode == safeMode) {
                    setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
                    abandonPendingAutoTransitionIfStopped()
                }
            } finally {
                DiagnosticLog.critical(
                    "auto-vpn",
                    "initial-scan-finish mode=$safeMode pendingTransition=$pendingAutoServiceTransition " +
                            "started=${SagerNet.started} progress=$autoFinished/$autoTotal working=$autoWorking"
                )
                finishAutoProgress()
                if (!pendingAutoServiceTransition && !SagerNet.started) {
                    onFinishedWithoutServiceStart?.invoke()
                }
            }
        }
        return true
    }

    fun resetGeminiRejectedProfiles(): Int {
        val count = DataStore.autoVpnGeminiRejectedKeys.size
        DataStore.autoVpnGeminiRejectedKeys = emptySet()
        if (DataStore.autoVpnEnabled && DataStore.autoVpnMode == MODE_GEMINI) {
            synchronized(workingPools) {
                workingPools.remove(MODE_GEMINI)
                hydratedReserveModes.remove(MODE_GEMINI)
            }
            DataStore.autoVpnReserveIdsGemini = ""
            DataStore.autoVpnReserveCountGemini = 0
            workingCache.clear()
            scannedIds.remove(MODE_GEMINI)
            setAutoReadyStatus(MODE_GEMINI)
        }
        return count
    }

    fun resetExitCountryRejectedProfiles(): Int {
        val count = synchronized(exitCountryRejectedWriteLock) {
            val rejectedCount = exitCountryRejectedKeys().size
            DataStore.autoVpnExitCountryRejectedKeys = emptySet()
            updateExitCountryRejectedCache(emptySet(), bumpExitCountryRejectedRevision())
            rejectedCount
        }
        clearExitCountryTransientExclusions()
        if (DataStore.autoVpnEnabled && requiresExitCountryCheck(DataStore.autoVpnMode)) {
            setAutoReadyStatus(DataStore.autoVpnMode)
        }
        notifyAutoStateChanged()
        return count
    }

    /**
     * Runs in the dedicated direct-test foreground process. That process survives
     * stopping the VPN service, so candidate results are never measured through an
     * active tunnel and a handoff cannot leave Auto in a stale searching state.
     */
    internal suspend fun runDetachedDirectCandidateSearch(
        mode: Int,
        excludedProfileId: Long,
        reason: String,
        resetResults: Boolean,
    ): Boolean {
        val safeMode = sanitizeMode(mode)
        if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode || isVpnRunningForAuto()) {
            DiagnosticLog.warn(
                "auto-vpn",
                "detached-search-skipped reason=$reason mode=$safeMode active=${isVpnRunningForAuto()}"
            )
            return false
        }

        DiagnosticLog.critical(
            "auto-vpn",
            "detached-search-start reason=$reason mode=$safeMode reset=$resetResults excluded=$excludedProfileId"
        )
        try {
            syncVisibleStateFromStore()
            ProfileTestRunner.syncFromStore()
            if (!ProfileTestRunner.isRunning()) {
                ProfileTestRunner.clearState()
            }

            val groupIds = enabledGroupIds()
            if (groupIds.isEmpty()) {
                clearAutoProgress()
                setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
                abandonPendingAutoTransitionIfStopped()
                if (resetResults) onMainDispatcher { SagerNet.stopService() }
                return false
            }

            if (resetResults) {
                resetAutoSearchState(cancelScan = false)
                resetAutoCandidateResults(groupIds, safeMode)
            }

            val excludedIds = automaticExcludedIds(
                safeMode,
                listOf(excludedProfileId).filter { it > 0L },
            )

            if (!resetResults) {
                val reserveCandidate = reserveProfileIds(safeMode)
                    .asSequence()
                    .filter { it !in excludedIds }
                    .mapNotNull { SagerDatabase.proxyDao.getById(it) }
                    .filter { profileAllowed(it, safeMode) && !isMarkedFailed(it.id) }
                    .minWithOrNull(candidateComparator(safeMode))
                if (reserveCandidate != null) {
                    DiagnosticLog.critical(
                        "auto-vpn",
                        "detached-search-precheck-reserve reason=$reason " +
                                ProfileTestRunner.profileDiagnosticSummary(reserveCandidate)
                    )
                    if (switchToVerified(
                            initial = CandidateResult(reserveCandidate, reserveCandidate.ping),
                            mode = safeMode,
                            excludedIds = excludedIds,
                            forceTest = true,
                            startIfStopped = true,
                            feedback = true,
                            recordManualCycle = false,
                            switchReason = "detached-$reason",
                        )
                    ) {
                        setAutoReadyStatus(safeMode)
                        return true
                    }
                }
            }

            val total = SagerDatabase.proxyDao.countByGroups(groupIds)
                .coerceAtMost(Int.MAX_VALUE.toLong())
                .toInt()
            if (resetResults) {
                resetAutoProgress(total)
            } else {
                beginOnDemandSearchProgress(safeMode, total)
            }
            setAutoStatus(safeMode, R.string.auto_vpn_status_initial_search)
            val working = scanInitialWorkingProfiles(
                groupIds = groupIds,
                excludedIds = excludedIds,
                mode = safeMode,
                target = reserveTarget(),
                countProgress = true,
            )
            val best = chooseBestCandidate(working, safeMode)
            if (best == null) {
                setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
                abandonPendingAutoTransitionIfStopped()
                if (resetResults) onMainDispatcher { SagerNet.stopService() }
                return false
            }

            return switchMutex.withLock {
                switchToVerified(
                    initial = best,
                    mode = safeMode,
                    excludedIds = excludedIds,
                    forceTest = true,
                    startIfStopped = true,
                    feedback = true,
                    recordManualCycle = false,
                    switchReason = "detached-search-$reason",
                ).also { switched ->
                    if (switched) setAutoReadyStatus(safeMode)
                }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            DiagnosticLog.error("auto-vpn", "detached-search-error reason=$reason mode=$safeMode", e)
            setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
            abandonPendingAutoTransitionIfStopped()
            if (resetResults) onMainDispatcher { SagerNet.stopService() }
            return false
        } finally {
            finishAutoProgress()
            DiagnosticLog.critical(
                "auto-vpn",
                "detached-search-finish reason=$reason mode=$safeMode progress=$autoFinished/$autoTotal " +
                        "working=$autoWorking started=${SagerNet.started}"
            )
        }
    }

    internal fun reportDetachedSearchStopTimeout(mode: Int, reason: String) {
        val safeMode = sanitizeMode(mode)
        DiagnosticLog.error(
            "auto-vpn",
            "detached-search-stop-timeout reason=$reason mode=$safeMode ${DiagnosticLog.stateSummary()}"
        )
        if (DataStore.autoVpnEnabled && DataStore.autoVpnMode == safeMode) {
            setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
            finishAutoProgress()
        }
        pendingAutoServiceTransition = false
        DataStore.autoVpnServiceStartRequested = false
    }

    internal fun stopDetachedDirectSearch() {
        pendingAutoServiceTransition = false
        DataStore.autoVpnServiceStartRequested = false
        finishAutoProgress()
        setAutoStatus(DataStore.autoVpnMode, R.string.auto_vpn_status_scan_stopped)
    }

    /*
     * Kept for older callers; the public flow above now starts from the VPN button.
     */
    private fun startLegacyAutoSelection() {
        runOnDefaultDispatcher {
            switchMutex.withLock {
                val safeMode = sanitizeMode(DataStore.autoVpnMode)
                DataStore.autoVpnMode = safeMode
                manualCycleIds.getOrPut(safeMode) { LinkedHashSet() }.clear()
                val working = prepareWorkingPool(safeMode)
                if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode) return@withLock
                val best = chooseBestCandidate(working, safeMode)
                if (best == null) {
                    setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
                    return@withLock
                }
                switchToVerified(
                    initial = best,
                    mode = safeMode,
                    excludedIds = emptySet(),
                    forceTest = true,
                    startIfStopped = true,
                    feedback = true,
                    recordManualCycle = false,
                    switchReason = "legacy-auto-selection",
                )
            }
        }
    }

    fun disable() {
        DataStore.autoVpnEnabled = false
        DataStore.autoVpnServiceStartRequested = false
        immediateVerifyGeneration.incrementAndGet()
        failures = 0
        monitorJob?.cancel()
        monitorJob = null
        immediateVerifyJob?.cancel()
        immediateVerifyJob = null
        clearServiceAvailability()
        autoScanJob?.cancel()
        autoScanJob = null
        stopTrafficMonitoring()
        activeTrafficSampler = null
        FloatingSwitchButton.hide()
        AutoProfileScoreStore.flush()
        clearAutoProgress()
        setAutoStatus(null)
    }

    fun onManualProfileSelected() {
        if (DataStore.autoVpnEnabled) {
            disable()
        }
    }

    fun shouldStartAutoSelectionInsteadOfService(): Boolean {
        return DataStore.autoVpnEnabled &&
                !pendingAutoServiceTransition &&
                !DataStore.autoVpnServiceStartRequested
    }

    fun isServiceTransitionPending(): Boolean {
        return pendingAutoServiceTransition
    }

    fun onVpnStarted(
        urlTester: (() -> Int)? = null,
        trafficSampler: (suspend () -> TrafficStats?)? = null,
    ) {
        refreshNetworkStatus("vpn-start-or-profile-switch")
        if (!DataStore.autoVpnEnabled) {
            FloatingSwitchButton.hide()
            return
        }
        DiagnosticLog.critical(
            "auto-vpn",
            "vpn-started-callback profile=${DataStore.startedProfile} mode=${sanitizeMode(DataStore.autoVpnMode)} " +
                    "urlTester=${urlTester != null} monitorIntervalMs=${checkIntervalMillis()} " +
                    "postConnectDelaysMs=${POST_CONNECT_VERIFY_DELAYS_MS.joinToString(",")}",
        )
        activeUrlTester = urlTester
        activeTrafficSampler = trafficSampler
        DataStore.autoVpnServiceStartRequested = false
        FloatingSwitchButton.sync()
        startMonitor()
        startTrafficMonitor()
        if (pendingAutoServiceTransition) {
            pendingAutoServiceTransition = false
        }
        verifyCurrentSoon(urlTester)
        scheduleServiceAvailabilityFallback(DataStore.startedProfile, sanitizeMode(DataStore.autoVpnMode))
    }

    fun testActiveUrl(): Int {
        val proxy = ServiceAvailabilityProbe.activeProfileProxy()
            ?: throw IOException("Active VPN route is unavailable")
        return ActiveVpnTrafficProbe.requireHttp204(
            DataStore.connectionTestURL,
            proxy,
            timeoutMillis = 5_000,
        )
    }

    fun onVpnStopped() {
        val autoRestarting = pendingAutoServiceTransition || DataStore.autoVpnServiceStartRequested
        DiagnosticLog.critical(
            "auto-vpn",
            "vpn-stopped-callback profile=${DataStore.startedProfile} mode=${sanitizeMode(DataStore.autoVpnMode)} " +
                    "autoRestarting=$autoRestarting immediateVerifyActive=${immediateVerifyJob?.isActive == true} " +
                    "monitorActive=${monitorJob?.isActive == true}",
        )
        failures = 0
        immediateVerifyGeneration.incrementAndGet()
        monitorJob?.cancel()
        monitorJob = null
        immediateVerifyJob?.cancel()
        immediateVerifyJob = null
        clearServiceAvailability()
        stopTrafficMonitoring()
        activeUrlTester = null
        activeTrafficSampler = null
        FloatingSwitchButton.hide()
        if (!autoRestarting) {
            resetAutoSearchState(cancelScan = true)
            setAutoStatus(null)
        }
    }

    fun onServiceFailed(message: String?) {
        if (!DataStore.autoVpnEnabled || message == null) return
        val safeMode = sanitizeMode(DataStore.autoVpnMode)
        val failedProfiles = currentProfileIds()
        for (failedProfile in failedProfiles) {
            markFailed(failedProfile)
        }
        runOnDefaultDispatcher {
            if (shouldCountScoreFailure(IOException(message))) {
                failedProfiles.forEach { profileId ->
                    SagerDatabase.proxyDao.getById(profileId)?.let {
                        AutoProfileScoreStore.recordFailure(
                            it,
                            safeMode,
                            AutoProfileScoreStore.Event.POST_CONNECT,
                        )
                    }
                }
            } else {
                DiagnosticLog.event(
                    "auto-vpn",
                    "profile-score-service-failure-ignored-no-direct-network mode=$safeMode profiles=${failedProfiles.size}",
                )
            }
        }
        if (!isVpnRunningForAuto() && AutoVpnDirectSearchService.startCandidateSearch(
                context = app,
                mode = safeMode,
                profileId = failedProfiles.firstOrNull() ?: 0L,
                reason = "service-failed",
            )
        ) {
            setAutoStatus(safeMode, R.string.auto_vpn_status_selecting)
            return
        }
        switchToBestProfile(
            mode = safeMode,
            excludedIds = failedProfiles.toSet(),
            forceTest = true,
            startIfStopped = true,
            ignoreSwitchCooldown = true,
            feedback = true,
            switchReason = "service-failed",
        )
    }

    fun switchNowFromNotification() {
        setAutoStatus(DataStore.autoVpnMode, R.string.auto_vpn_status_selecting)
        switchToNextProfile(feedback = true)
    }

    fun switchNowFromApp() {
        if (SagerNet.started || DataStore.startedProfile > 0L) {
            app.sendBroadcast(Intent(Action.AUTO_SWITCH).setPackage(app.packageName))
            return
        }
        setAutoStatus(DataStore.autoVpnMode, R.string.auto_vpn_status_selecting)
        switchToNextProfile(feedback = true)
    }

    fun switchGeminiSoftFromNotification() {
        setAutoStatus(MODE_GEMINI, R.string.auto_vpn_status_selecting)
        switchGeminiSoft(feedback = true)
    }

    fun switchFromFloatingButton() {
        if (isGeminiModeActive()) {
            switchGeminiSoftFromNotification()
        } else {
            switchNowFromNotification()
        }
    }

    fun switchGeminiSoftFromApp() {
        if (SagerNet.started || DataStore.startedProfile > 0L) {
            app.sendBroadcast(Intent(Action.AUTO_SWITCH_GEMINI_SOFT).setPackage(app.packageName))
            return
        }
        setAutoStatus(MODE_GEMINI, R.string.auto_vpn_status_selecting)
        switchGeminiSoft(feedback = true)
    }

    fun stopScanFromApp() {
        AutoVpnDirectSearchService.stop(app)
        if (SagerNet.started || DataStore.startedProfile > 0L || DataStore.autoVpnProgressRunning) {
            app.sendBroadcast(Intent(Action.AUTO_SCAN_STOP).setPackage(app.packageName))
            return
        }
        stopScan(feedback = true)
    }

    fun stopScanFromService() {
        stopScan(feedback = true)
    }

    fun resumeScanFromApp() {
        if (SagerNet.started || DataStore.startedProfile > 0L) {
            app.sendBroadcast(Intent(Action.AUTO_SCAN_RESUME).setPackage(app.packageName))
            return
        }
        startSelectedModeFromVpnButton()
    }

    fun resumeScanFromService() {
        if (!DataStore.autoVpnEnabled) return
        if (!SagerNet.started && DataStore.startedProfile <= 0L) {
            startSelectedModeFromVpnButton()
        } else {
            setAutoReadyStatus()
        }
    }

    fun onSubscriptionsUpdated() {
        if (!DataStore.autoVpnEnabled || !SagerNet.started) return
        verifyCurrentAndSwitchIfBad(countFailure = true, ignoreSwitchCooldown = false, feedback = false)
    }

    private fun switchToNextProfile(feedback: Boolean) {
        DataStore.autoVpnEnabled = true
        DataStore.autoVpnMode = sanitizeMode(DataStore.autoVpnMode)
        runOnDefaultDispatcher {
            switchMutex.withLock {
                if (!DataStore.autoVpnEnabled) return@withLock
                val mode = sanitizeMode(DataStore.autoVpnMode)
                val currentIds = currentProfileIds()
                val visited = manualCycleIds.getOrPut(mode) { LinkedHashSet() }
                visited += currentIds
                currentIds.forEach { markManualSkipped(it, mode) }
                if (mode == MODE_GEMINI) {
                    rememberGeminiRejectedProfiles(currentIds)
                }

                val excludedIds = automaticExcludedIds(mode, currentIds + visited)
                val best = findBestCandidate(excludedIds, forceTest = true, mode = mode)
                if (best == null) {
                    setAutoStatus(mode, R.string.auto_vpn_status_no_candidate)
                    abandonPendingAutoTransitionIfStopped()
                    return@withLock
                }

                switchToVerified(
                        initial = best,
                        mode = mode,
                        excludedIds = excludedIds,
                        forceTest = true,
                        startIfStopped = true,
                        feedback = feedback,
                        recordManualCycle = true,
                        switchReason = "manual-next-profile",
                    )
            }
        }
    }

    private fun switchGeminiSoft(feedback: Boolean) {
        DataStore.autoVpnEnabled = true
        DataStore.autoVpnMode = MODE_GEMINI
        runOnDefaultDispatcher {
            switchMutex.withLock {
                if (!DataStore.autoVpnEnabled) return@withLock
                DataStore.autoVpnMode = MODE_GEMINI
                val currentIds = currentProfileIds()
                currentIds.forEach { profileId ->
                    SagerDatabase.proxyDao.getById(profileId)?.let {
                        AutoProfileScoreStore.recordManualSwitch(it, MODE_GEMINI)
                    }
                }
                val best = findBestCandidate(
                    excludedIds = automaticExcludedIds(MODE_GEMINI, currentIds),
                    forceTest = true,
                    mode = MODE_GEMINI,
                )
                if (best == null) {
                    setAutoStatus(MODE_GEMINI, R.string.auto_vpn_status_no_candidate)
                    abandonPendingAutoTransitionIfStopped()
                    return@withLock
                }
                if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != MODE_GEMINI) return@withLock

                switchToVerified(
                        initial = best,
                        mode = MODE_GEMINI,
                        excludedIds = automaticExcludedIds(MODE_GEMINI, currentIds),
                        forceTest = true,
                        startIfStopped = true,
                        feedback = feedback,
                        recordManualCycle = false,
                        switchReason = "manual-gemini-soft-switch",
                    )
            }
        }
    }

    private fun switchToBestProfile(
        mode: Int,
        excludedIds: Set<Long>,
        forceTest: Boolean,
        startIfStopped: Boolean,
        ignoreSwitchCooldown: Boolean,
        feedback: Boolean,
        switchReason: String = "automatic-recovery",
    ) {
        runOnDefaultDispatcher {
            switchMutex.withLock {
                if (!DataStore.autoVpnEnabled) return@withLock
                val safeMode = sanitizeMode(mode)
                DataStore.autoVpnMode = safeMode
                val now = System.currentTimeMillis()
                if (!ignoreSwitchCooldown && now - lastSwitchAt < switchCooldownMillis()) {
                    return@withLock
                }

                val effectiveExcluded = automaticExcludedIds(safeMode, excludedIds)
                val best = findBestCandidate(effectiveExcluded, forceTest, safeMode)
                if (best == null) {
                    setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
                    abandonPendingAutoTransitionIfStopped()
                    return@withLock
                }
                if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode) return@withLock

                switchToVerified(
                        initial = best,
                        mode = safeMode,
                        excludedIds = effectiveExcluded,
                        forceTest = true,
                        startIfStopped = startIfStopped,
                        feedback = feedback,
                        recordManualCycle = false,
                        switchReason = switchReason,
                    )
            }
        }
    }

    private suspend fun prepareWorkingPool(mode: Int): List<CandidateResult> {
        val groupIds = enabledGroupIds()
        if (groupIds.isEmpty()) return emptyList()

        resetAutoCandidateResults(groupIds, mode)
        resetAutoProgress(SagerDatabase.proxyDao.countByGroups(groupIds).coerceAtMost(Int.MAX_VALUE.toLong()).toInt())
        val working = scanWorkingProfiles(
            groupIds = groupIds,
            excludedIds = emptySet(),
            mode = mode,
            target = reserveTarget(),
            countProgress = true,
            connectFirst = false,
            startIfStopped = false,
            feedback = false,
            scanAllAvailable = true,
        )
        finishAutoProgress()
        return working
    }

    private suspend fun scanInitialWorkingProfiles(
        groupIds: List<Long>,
        excludedIds: Set<Long>,
        mode: Int,
        target: Int,
        countProgress: Boolean,
    ): List<CandidateResult> {
        val safeMode = sanitizeMode(mode)
        val parallel = DataStore.connectionTestParallel.coerceAtLeast(1)
        val throttle = autoTestThrottle()
        val working = ArrayList<CandidateResult>()
        val selectionWindowMillis = initialSelectionWindowMillis()
        val firstWorkingLock = Any()
        var firstWorkingAt = 0L

        fun noteFirstWorking(profileId: Long) {
            synchronized(firstWorkingLock) {
                if (firstWorkingAt == 0L) {
                    firstWorkingAt = System.currentTimeMillis()
                    DiagnosticLog.critical(
                        "auto-vpn",
                        "initial-selection-window-start mode=$safeMode profile=$profileId " +
                                "timeoutMs=$selectionWindowMillis",
                    )
                }
            }
        }

        fun selectionWindowExpired(): Boolean {
            val startedAt = synchronized(firstWorkingLock) { firstWorkingAt }
            return startedAt > 0L && System.currentTimeMillis() - startedAt >= selectionWindowMillis
        }

        setAutoStatus(safeMode, R.string.auto_vpn_status_initial_search)

        while (DataStore.autoVpnEnabled && DataStore.autoVpnMode == safeMode && working.size < target) {
            if (selectionWindowExpired()) {
                DiagnosticLog.critical(
                    "auto-vpn",
                    "initial-selection-window-expired mode=$safeMode working=${working.size}/$target",
                )
                break
            }

            // Keep one stable ordering for the whole round. The delay sort changes as tests
            // complete, so OFFSET paging could otherwise make a round appear to restart early.
            val scanPhases = AutoProfileScoreStore.partitionForScan(autoScanProfiles(groupIds), safeMode)
            val scannedTotal = scanPhases.totalProfiles
            if (scannedTotal == 0) break
            var testedAnyInCycle = false
            var keepScanning = true

            phaseLoop@ for (phaseProfiles in listOf(scanPhases.vip, scanPhases.regular)) {
                if (phaseProfiles.isEmpty()) continue
                if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode ||
                    working.size >= target
                ) {
                    keepScanning = false
                    break@phaseLoop
                }

                for (page in phaseProfiles.chunked(AUTO_SCAN_PAGE_SIZE)) {
                    if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode ||
                        working.size >= target
                    ) {
                        keepScanning = false
                        break@phaseLoop
                    }
                    if (selectionWindowExpired()) {
                        keepScanning = false
                        break@phaseLoop
                    }

                    val dynamicExcluded = excludedIds + currentProfileIds() + working.map { it.profile.id }
                    val candidates = page.asSequence()
                        .filter { it.id !in dynamicExcluded }
                        .filter { profileAllowed(it, safeMode) }
                        .filterNot { isMarkedFailed(it.id) }
                        .toList()
                    if (countProgress) recordAutoSkippedResults(page.size - candidates.size)

                    for (chunk in candidates.chunked(parallel)) {
                        if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode ||
                            working.size >= target
                        ) {
                            keepScanning = false
                            break@phaseLoop
                        }
                        if (selectionWindowExpired()) {
                            keepScanning = false
                            break@phaseLoop
                        }
                        val testChunk = chunk.filter { markScanned(safeMode, it.id) }
                        if (countProgress) recordAutoSkippedResults(chunk.size - testChunk.size)
                        if (testChunk.isEmpty()) continue
                        testedAnyInCycle = true
                        val tested = sortCandidatesByScore(
                            testCandidates(
                                testChunk,
                                safeMode,
                                countProgress,
                                throttle,
                                onWorkingResult = {
                                    noteFirstWorking(it.profile.id)
                                    rememberWorkingResults(safeMode, listOf(it))
                                },
                            ),
                            safeMode,
                        )
                        if (tested.isEmpty()) continue

                        rememberWorkingResults(safeMode, tested)
                        working += tested
                        setAutoStatus(safeMode, R.string.auto_vpn_status_testing_more)
                    }
                }
            }

            if (selectionWindowExpired()) {
                DiagnosticLog.critical(
                    "auto-vpn",
                    "initial-selection-window-expired mode=$safeMode working=${working.size}/$target",
                )
                break
            }
            if (!keepScanning || working.size >= target) break
            if (!restartAutoScanCycle(safeMode, scannedTotal, testedAnyInCycle)) break
        }

        return sortCandidatesByScore(working, safeMode)
    }

    private suspend fun scanWorkingProfiles(
        groupIds: List<Long>,
        excludedIds: Set<Long>,
        mode: Int,
        target: Int,
        countProgress: Boolean,
        connectFirst: Boolean,
        startIfStopped: Boolean,
        feedback: Boolean,
        scanAllAvailable: Boolean = false,
    ): List<CandidateResult> {
        val safeMode = sanitizeMode(mode)
        val parallel = DataStore.connectionTestParallel.coerceAtLeast(1)
        val throttle = autoTestThrottle()
        val working = ArrayList<CandidateResult>()
        var connected = false
        setAutoStatus(safeMode, R.string.auto_vpn_status_testing_more)

        while (DataStore.autoVpnEnabled &&
            DataStore.autoVpnMode == safeMode &&
            (scanAllAvailable || reserveCount(safeMode) < target)
        ) {
            val scanPhases = AutoProfileScoreStore.partitionForScan(autoScanProfiles(groupIds), safeMode)
            val scannedTotal = scanPhases.totalProfiles
            if (scannedTotal == 0) break
            var testedAnyInCycle = false
            var keepScanning = true

            phaseLoop@ for ((phaseIndex, phaseProfiles) in listOf(scanPhases.vip, scanPhases.regular).withIndex()) {
                val vipPhase = phaseIndex == 0
                if (phaseProfiles.isEmpty()) continue
                if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode ||
                    (!vipPhase && !scanAllAvailable && reserveCount(safeMode) >= target)
                ) {
                    keepScanning = false
                    break@phaseLoop
                }

                for (page in phaseProfiles.chunked(AUTO_SCAN_PAGE_SIZE)) {
                    if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode ||
                        (!vipPhase && !scanAllAvailable && reserveCount(safeMode) >= target)
                    ) {
                        keepScanning = false
                        break@phaseLoop
                    }

                    val dynamicExcluded = excludedIds + currentProfileIds() + reserveProfileIds(safeMode)
                    val candidates = page.asSequence()
                        .filter { it.id !in dynamicExcluded }
                        .filter { profileAllowed(it, safeMode) }
                        .filterNot { isMarkedFailed(it.id) }
                        .toList()
                    if (countProgress) recordAutoSkippedResults(page.size - candidates.size)

                    for (chunk in candidates.chunked(parallel)) {
                        if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode ||
                            (!vipPhase && !scanAllAvailable && reserveCount(safeMode) >= target)
                        ) {
                            keepScanning = false
                            break@phaseLoop
                        }
                        val testChunk = chunk.filter { markScanned(safeMode, it.id) }
                        if (countProgress) recordAutoSkippedResults(chunk.size - testChunk.size)
                        if (testChunk.isEmpty()) continue
                        testedAnyInCycle = true
                        val tested = sortCandidatesByScore(
                            testCandidates(
                                testChunk,
                                safeMode,
                                countProgress,
                                throttle,
                                onWorkingResult = { rememberWorkingResults(safeMode, listOf(it)) },
                            ),
                            safeMode,
                        )
                        if (tested.isEmpty()) continue

                        rememberWorkingResults(safeMode, tested)
                        working += tested

                        if (connectFirst && !connected) {
                            val best = chooseBestCandidate(working, safeMode) ?: tested.first()
                            switchMutex.withLock {
                                if (DataStore.autoVpnEnabled && DataStore.autoVpnMode == safeMode) {
                                    connected = switchToVerified(
                                        initial = best,
                                        mode = safeMode,
                                        excludedIds = dynamicExcluded,
                                        forceTest = true,
                                        startIfStopped = startIfStopped,
                                        feedback = feedback,
                                        recordManualCycle = false,
                                        switchReason = "reserve-fill",
                                    )
                                }
                            }
                            setAutoStatus(safeMode, R.string.auto_vpn_status_testing_more)
                        }
                    }
                }
            }

            if (!keepScanning || (!scanAllAvailable && reserveCount(safeMode) >= target)) break
            if (!restartAutoScanCycle(safeMode, scannedTotal, testedAnyInCycle)) break
        }

        return sortCandidatesByScore(working, safeMode)
    }

    private fun startMonitor() {
        if (monitorJob?.isActive == true) return
        monitorJob = runOnDefaultDispatcher {
            val monitorStartedAt = System.currentTimeMillis()
            var tick = 0
            while (isActive && DataStore.autoVpnEnabled) {
                delay(checkIntervalMillis())
                if (!DataStore.autoVpnEnabled) break
                if (!SagerNet.started || DataStore.startedProfile <= 0L) continue
                tick += 1
                DiagnosticLog.critical(
                    "auto-vpn",
                    "active-monitor-tick tick=$tick elapsedMs=${System.currentTimeMillis() - monitorStartedAt} " +
                            "profile=${DataStore.startedProfile} immediateVerifyActive=${immediateVerifyJob?.isActive == true}",
                )
                if (immediateVerifyJob?.isActive == true) {
                    DiagnosticLog.event(
                        "auto-vpn",
                        "active-monitor-skipped-during-post-connect profile=${DataStore.startedProfile}",
                    )
                    continue
                }
                try {
                    val profileId = DataStore.startedProfile
                    val mode = sanitizeMode(DataStore.autoVpnMode)
                    val activeCheck = checkActiveProfile(verifyTrafficPath = true)
                    if (!activeCheck.bad) {
                        failures = 0
                        if (!hasServiceAvailabilityState(profileId, mode)) setAutoReadyStatus(mode)
                    } else {
                        if (!hasDirectInternet()) {
                            deferForMissingDirectNetwork(profileId, mode, trafficTriggered = false)
                            failures = 0
                            continue
                        }
                        failures = (failures + 1).coerceAtMost(failuresBeforeSwitch())
                        setActiveFailureStatus(mode, activeCheck.ping, failures, failuresBeforeSwitch())
                        if (failures < failuresBeforeSwitch()) {
                            if (verifyActiveRetrySequence(profileId, mode, "scheduled")) {
                                failures = 0
                            } else if (isSameStartedAutoProfile(profileId, mode)) {
                                if (!hasDirectInternet()) {
                                    deferForMissingDirectNetwork(profileId, mode, trafficTriggered = false)
                                    failures = 0
                                } else {
                                    failures = (failures + 1).coerceAtMost(failuresBeforeSwitch())
                                    setActiveFailureStatus(
                                        mode,
                                        lastCheckedPingMillis,
                                        failures,
                                        failuresBeforeSwitch(),
                                    )
                                }
                            }
                        }
                    }

                    if (failures >= failuresBeforeSwitch()) {
                        val failedProfile = DataStore.startedProfile
                        markFailed(failedProfile, countFailure = false)
                        failures = 0
                        switchToBestProfile(
                            mode = DataStore.autoVpnMode,
                            excludedIds = setOf(failedProfile),
                            forceTest = false,
                            startIfStopped = false,
                            ignoreSwitchCooldown = true,
                            feedback = false,
                            switchReason = "active-monitor-failed",
                        )
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    DiagnosticLog.error("auto-vpn", "active-monitor-tick-failed", e)
                    Logs.w(e)
                    val mode = sanitizeMode(DataStore.autoVpnMode)
                    val checkingStatus = app.getString(
                        R.string.auto_vpn_status_checking_active,
                        modeName(mode),
                    )
                    if (isSameStartedAutoProfile(DataStore.startedProfile, mode) &&
                        DataStore.autoVpnStatusText == checkingStatus
                    ) {
                        setAutoReadyStatus(mode, scheduleServices = false)
                    }
                }
            }
        }
    }

    private suspend fun hasDirectInternet(): Boolean {
        val listenerKey = Any()
        val network = CompletableDeferred<Network?>()
        var listenerStarted = false
        return try {
            DefaultNetworkListener.start(listenerKey) { available ->
                if (available != null) network.complete(available)
            }
            listenerStarted = true
            withTimeoutOrNull(DIRECT_NETWORK_CHECK_TIMEOUT_MS) {
                val directNetwork = network.await() ?: return@withTimeoutOrNull false
                val capabilities = SagerNet.connectivity.getNetworkCapabilities(directNetwork)
                    ?: return@withTimeoutOrNull false
                if (!capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)) {
                    return@withTimeoutOrNull false
                }

                onIoDispatcher {
                    val connection = directNetwork.openConnection(URL(DIRECT_NETWORK_CHECK_URL)) as? HttpURLConnection
                        ?: return@onIoDispatcher false
                    try {
                        connection.instanceFollowRedirects = true
                        connection.requestMethod = "HEAD"
                        connection.connectTimeout = DIRECT_NETWORK_CONNECT_TIMEOUT_MS
                        connection.readTimeout = DIRECT_NETWORK_READ_TIMEOUT_MS
                        connection.responseCode in 200..399
                    } finally {
                        connection.disconnect()
                    }
                }
            } == true
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            DiagnosticLog.warn("auto-vpn", "direct-network-check-failed", e)
            false
        } finally {
            if (listenerStarted) runCatching { DefaultNetworkListener.stop(listenerKey) }
        }
    }

    private fun deferForMissingDirectNetwork(profileId: Long, mode: Int, trafficTriggered: Boolean) {
        if (!isSameStartedAutoProfile(profileId, mode)) return
        failures = 0
        setAutoStatus(mode, R.string.auto_vpn_status_underlying_network_unavailable)
        DiagnosticLog.event(
            "auto-vpn",
            "active-check-deferred-no-underlying-network profile=$profileId mode=$mode " +
                    "trafficTriggered=$trafficTriggered",
        )
    }

    private suspend fun confirmActiveProfileFailure(profileId: Long, mode: Int, reason: String) {
        if (!isSameStartedAutoProfile(profileId, mode)) return
        DiagnosticLog.critical(
            "auto-vpn",
            "active-failure-confirmed reason=$reason profile=$profileId mode=$mode",
        )
        switchAfterPostConnectFailures(profileId, mode)
    }

    private fun startTrafficMonitor() {
        if (!DataStore.autoVpnTrafficMonitorEnabled) {
            stopTrafficMonitoring()
            return
        }
        if (trafficMonitorJob?.isActive == true) return
        trafficMonitorJob = runOnDefaultDispatcher {
            var previous: TrafficStats? = null
            var previousProfileId = 0L
            var suspiciousIntervals = 0

            while (isActive && DataStore.autoVpnEnabled && DataStore.autoVpnTrafficMonitorEnabled) {
                delay(TRAFFIC_SAMPLE_INTERVAL_MS)
                if (!DataStore.autoVpnEnabled || !DataStore.autoVpnTrafficMonitorEnabled ||
                    !SagerNet.started || DataStore.startedProfile <= 0L
                ) {
                    previous = null
                    previousProfileId = 0L
                    suspiciousIntervals = 0
                    continue
                }

                val profileId = DataStore.startedProfile
                val sample = runCatching { activeTrafficSampler?.invoke() }.getOrNull()
                if (sample == null) {
                    previous = null
                    previousProfileId = 0L
                    suspiciousIntervals = 0
                    continue
                }

                val old = previous
                if (old == null || previousProfileId != profileId) {
                    previous = sample
                    previousProfileId = profileId
                    suspiciousIntervals = 0
                    continue
                }

                val outgoing = (sample.txTotal - old.txTotal).coerceAtLeast(0L)
                val incoming = (sample.rxTotal - old.rxTotal).coerceAtLeast(0L)
                previous = sample
                if (outgoing >= TRAFFIC_OUTGOING_MIN_BYTES && incoming <= TRAFFIC_INCOMING_MAX_BYTES) {
                    suspiciousIntervals += 1
                } else {
                    suspiciousIntervals = 0
                }

                if (suspiciousIntervals >= TRAFFIC_SUSPICIOUS_INTERVALS) {
                    suspiciousIntervals = 0
                    startTrafficDiagnostic(profileId, sanitizeMode(DataStore.autoVpnMode))
                }
            }
        }
    }

    private fun stopTrafficMonitoring() {
        trafficMonitorJob?.cancel()
        trafficMonitorJob = null
        trafficDiagnosticJob?.cancel()
        trafficDiagnosticJob = null
        lastTrafficDiagnosticAt = 0L
    }

    private fun startTrafficDiagnostic(profileId: Long, mode: Int) {
        if (trafficDiagnosticJob?.isActive == true) return
        val now = System.currentTimeMillis()
        if (now - lastTrafficDiagnosticAt < TRAFFIC_DIAGNOSTIC_COOLDOWN_MS) return
        lastTrafficDiagnosticAt = now
        trafficDiagnosticJob = runOnDefaultDispatcher {
            try {
                diagnoseTrafficSignal(profileId, mode)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                DiagnosticLog.error("auto-vpn", "traffic-diagnostic-failed profile=$profileId mode=$mode", e)
            }
        }
    }

    private suspend fun diagnoseTrafficSignal(profileId: Long, mode: Int) {
        val safeMode = sanitizeMode(mode)
        if (!isSameStartedAutoProfile(profileId, safeMode)) return
        if (immediateVerifyJob?.isActive == true) {
            DiagnosticLog.event(
                "auto-vpn",
                "traffic-diagnostic-skipped-during-post-connect profile=$profileId mode=$safeMode",
            )
            return
        }
        DiagnosticLog.event("auto-vpn", "traffic-signal profile=$profileId mode=$safeMode")

        val first = checkActiveProfile(verifyTrafficPath = true)
        if (!first.bad || !isSameStartedAutoProfile(profileId, safeMode)) {
            failures = 0
            return
        }
        if (!hasDirectInternet()) {
            DiagnosticLog.event("auto-vpn", "traffic-signal-deferred-no-underlying-network profile=$profileId")
            return
        }

        failures = 1.coerceAtMost(failuresBeforeSwitch())
        setActiveFailureStatus(safeMode, first.ping, failures, failuresBeforeSwitch())
        if (failures >= failuresBeforeSwitch()) {
            failures = 0
            switchAfterPostConnectFailures(profileId, safeMode)
            return
        }
        if (verifyActiveRetrySequence(profileId, safeMode, "traffic-signal")) return
        if (!isSameStartedAutoProfile(profileId, safeMode)) return
        if (!hasDirectInternet()) {
            deferForMissingDirectNetwork(profileId, safeMode, trafficTriggered = true)
            failures = 0
            return
        }
        failures = (failures + 1).coerceAtMost(failuresBeforeSwitch())
        setActiveFailureStatus(safeMode, lastCheckedPingMillis, failures, failuresBeforeSwitch())
        if (failures >= failuresBeforeSwitch()) {
            failures = 0
            switchAfterPostConnectFailures(profileId, safeMode)
        }
    }

    private suspend fun verifyActiveRetrySequence(profileId: Long, mode: Int, source: String): Boolean {
        for ((index, delayMs) in POST_CONNECT_VERIFY_DELAYS_MS.withIndex()) {
            delay(delayMs)
            if (!isSameStartedAutoProfile(profileId, mode)) return false
            DiagnosticLog.event(
                "auto-vpn",
                "active-retry-start source=$source attempt=${index + 1}/${POST_CONNECT_VERIFY_DELAYS_MS.size} " +
                        "profile=$profileId mode=$mode",
            )
            setAutoNotificationStatus(
                mode,
                R.string.auto_vpn_status_verifying_connected,
                index + 1,
                POST_CONNECT_VERIFY_DELAYS_MS.size,
            )
            val result = checkActiveProfile(periodicRetry = true, verifyTrafficPath = true)
            if (!result.bad && isSameStartedAutoProfile(profileId, mode)) {
                failures = 0
                setAutoReadyStatus(mode, scheduleServices = false)
                refreshServiceAvailabilityAfterStableCheck(profileId, mode)
                DiagnosticLog.event(
                    "auto-vpn",
                    "active-retry-recovered source=$source attempt=${index + 1} profile=$profileId mode=$mode",
                )
                return true
            }
            setActiveFailureStatus(mode, result.ping, index + 1, POST_CONNECT_VERIFY_DELAYS_MS.size)
            DiagnosticLog.event(
                "auto-vpn",
                "active-retry-failed source=$source attempt=${index + 1}/${POST_CONNECT_VERIFY_DELAYS_MS.size} " +
                        "ping=${result.ping ?: "error"} countryRejected=${result.exitCountryRejected} " +
                        "profile=$profileId mode=$mode",
            )
        }
        return false
    }

    private suspend fun shouldCountScoreFailure(error: Throwable): Boolean {
        if (!ProfileTestRunner.isLikelyTransientNetworkFailure(error)) return true
        val now = System.currentTimeMillis()
        if (now - scoreNetworkCheckedAt < SCORE_NETWORK_STATUS_CACHE_MS) return scoreNetworkAvailable
        return scoreNetworkCheckMutex.withLock {
            val checkedAt = System.currentTimeMillis()
            if (checkedAt - scoreNetworkCheckedAt < SCORE_NETWORK_STATUS_CACHE_MS) {
                return@withLock scoreNetworkAvailable
            }
            scoreNetworkAvailable = hasDirectInternet()
            scoreNetworkCheckedAt = checkedAt
            scoreNetworkAvailable
        }
    }

    private fun verifyCurrentSoon(urlTester: (() -> Int)?) {
        val verificationGeneration = immediateVerifyGeneration.incrementAndGet()
        val previousVerifyActive = immediateVerifyJob?.isActive == true
        if (previousVerifyActive) {
            DiagnosticLog.event(
                "auto-vpn",
                "post-connect-verification-replaced profile=${DataStore.startedProfile} mode=${sanitizeMode(DataStore.autoVpnMode)}",
            )
        }
        immediateVerifyJob?.cancel()
        val profileId = DataStore.startedProfile
        val mode = sanitizeMode(DataStore.autoVpnMode)
        resetServiceAvailabilityForConnection(profileId, mode)
        val startedAt = System.currentTimeMillis()
        DiagnosticLog.critical(
            "auto-vpn",
            "post-connect-verification-scheduled profile=$profileId mode=$mode urlTester=${urlTester != null} " +
                    "delaysMs=${POST_CONNECT_VERIFY_DELAYS_MS.joinToString(",")}",
        )
        immediateVerifyJob = runOnDefaultDispatcher {
            var outcome = "completed"
            var pendingVerificationStatus: String? = null
            val verifyStartedAt = System.currentTimeMillis()
            try {
                if (profileId <= 0L || urlTester == null) {
                    outcome = if (profileId <= 0L) "skipped-no-profile" else "skipped-no-url-tester"
                    return@runOnDefaultDispatcher
                }
                var connectFailures = 0
                var serviceAvailabilityScheduled = false
                for ((index, delayMs) in POST_CONNECT_VERIFY_DELAYS_MS.withIndex()) {
                    delay(delayMs)
                    if (!isSameStartedAutoProfile(profileId, mode)) {
                        outcome = "profile-or-mode-changed-before-attempt-${index + 1}"
                        DiagnosticLog.event("auto-vpn", "post-connect-verification-skipped outcome=$outcome profile=$profileId")
                        return@runOnDefaultDispatcher
                    }
                    pendingVerificationStatus = app.getString(
                        R.string.auto_vpn_status_verifying_connected,
                        modeName(mode),
                        index + 1,
                        POST_CONNECT_VERIFY_DELAYS_MS.size,
                    )
                    setAutoNotificationStatus(mode, R.string.auto_vpn_status_verifying_connected, index + 1, POST_CONNECT_VERIFY_DELAYS_MS.size)
                    DiagnosticLog.critical(
                        "auto-vpn",
                        "post-connect-attempt-start attempt=${index + 1}/${POST_CONNECT_VERIFY_DELAYS_MS.size} " +
                                "elapsedMs=${System.currentTimeMillis() - verifyStartedAt} profile=$profileId mode=$mode",
                    )
                    var countryAccepted = true
                    if (index == 0 || requiresExitCountryCheck(mode)) {
                        val countryCheckStartedAt = System.currentTimeMillis()
                        countryAccepted = verifyConnectedExitCountry(profileId, mode)
                        DiagnosticLog.event(
                            "auto-vpn",
                            "post-connect-country-check-finished accepted=$countryAccepted " +
                                    "elapsedMs=${System.currentTimeMillis() - countryCheckStartedAt} profile=$profileId mode=$mode",
                        )
                    }
                    val pingStartedAt = System.currentTimeMillis()
                    val ping = testStartedConnection(
                        profileId,
                        mode,
                        urlTester,
                        scoreEvent = AutoProfileScoreStore.Event.CANDIDATE,
                    )
                    DiagnosticLog.event(
                        "auto-vpn",
                        "post-connect-attempt-result attempt=${index + 1}/${POST_CONNECT_VERIFY_DELAYS_MS.size} " +
                                "ping=${ping ?: "error"} elapsedMs=${System.currentTimeMillis() - pingStartedAt} " +
                                "profile=$profileId mode=$mode",
                    )
                    if (!serviceAvailabilityScheduled && countryAccepted && ping != null && ping <= badPingMillis()) {
                        refreshServiceAvailabilityAfterStableCheck(profileId, mode)
                        serviceAvailabilityScheduled = true
                    }
                    val traffic = try {
                        verifyRealTrafficPath(profileId, mode, "post-connect")
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        DiagnosticLog.error(
                            "auto-vpn-services",
                            "post-connect-traffic-probe-failed profile=$profileId mode=$mode attempt=${index + 1}",
                            e,
                        )
                        null
                    }
                    if (countryAccepted && ping != null && ping <= badPingMillis() && traffic != null) {
                        recordActiveTrafficSuccess(
                            profileId,
                            mode,
                            traffic,
                            scoreEvent = AutoProfileScoreStore.Event.POST_CONNECT,
                            scoreRecovery = true,
                        )
                        failures = 0
                        setAutoReadyStatus(mode, scheduleServices = false)
                        outcome = "ready"
                        return@runOnDefaultDispatcher
                    }
                    if (traffic == null && !hasDirectInternet()) {
                        outcome = "deferred-no-direct-network"
                        deferForMissingDirectNetwork(profileId, mode, trafficTriggered = false)
                        return@runOnDefaultDispatcher
                    }
                    if (traffic == null && countryAccepted) {
                        recordActiveTrafficFailure(
                            profileId,
                            mode,
                            AutoProfileScoreStore.Event.POST_CONNECT,
                            "post-connect-attempt-${index + 1}",
                        )
                    }
                    if (ping != null) {
                        setActiveFailureStatus(mode, ping, index + 1, POST_CONNECT_VERIFY_DELAYS_MS.size)
                    } else {
                        setActiveFailureStatus(mode, null, index + 1, POST_CONNECT_VERIFY_DELAYS_MS.size)
                    }
                    connectFailures += 1
                }

                if (connectFailures >= POST_CONNECT_VERIFY_DELAYS_MS.size) {
                    if (!hasDirectInternet()) {
                        outcome = "deferred-no-direct-network-after-attempts"
                        deferForMissingDirectNetwork(profileId, mode, trafficTriggered = false)
                        return@runOnDefaultDispatcher
                    }
                    outcome = "switched-after-three-attempts"
                    switchAfterPostConnectFailures(profileId, mode)
                }
            } catch (e: CancellationException) {
                outcome = "cancelled"
                throw e
            } catch (e: Exception) {
                outcome = "error-${e.javaClass.simpleName}"
                DiagnosticLog.error("auto-vpn", "post-connect-verification-failed profile=$profileId mode=$mode", e)
                if (verificationGeneration == immediateVerifyGeneration.get() &&
                    isSameStartedAutoProfile(profileId, mode)
                ) {
                    setAutoReadyStatus(mode, scheduleServices = false)
                }
                throw e
            } finally {
                val pendingStatus = pendingVerificationStatus
                if (verificationGeneration == immediateVerifyGeneration.get() &&
                    pendingStatus != null &&
                    isSameStartedAutoProfile(profileId, mode) &&
                    DataStore.autoVpnStatusText == pendingStatus
                ) {
                    DiagnosticLog.warn(
                        "auto-vpn",
                        "post-connect-status-stale-cleared profile=$profileId mode=$mode",
                    )
                    setAutoReadyStatus(mode)
                }
                DiagnosticLog.critical(
                    "auto-vpn",
                    "post-connect-verification-finished outcome=$outcome elapsedMs=${System.currentTimeMillis() - verifyStartedAt} " +
                            "profile=$profileId mode=$mode stillCurrent=${isSameStartedAutoProfile(profileId, mode)}",
                )
            }
        }
    }

    private fun isSameStartedAutoProfile(profileId: Long, mode: Int): Boolean {
        return DataStore.autoVpnEnabled &&
                DataStore.autoVpnMode == sanitizeMode(mode) &&
                SagerNet.started &&
                DataStore.startedProfile == profileId
    }

    private suspend fun checkActiveProfile(
        periodicRetry: Boolean = failures > 0,
        verifyTrafficPath: Boolean = false,
    ): ActiveCheckResult {
        val profileId = DataStore.startedProfile
        val mode = sanitizeMode(DataStore.autoVpnMode)
        val urlTester = activeUrlTester ?: return ActiveCheckResult(checkCurrentProfileBad())
        if (profileId <= 0L || !isSameStartedAutoProfile(profileId, mode)) return ActiveCheckResult(false)
        val checkGeneration = activeProfileCheckGeneration.incrementAndGet()
        if (!periodicRetry) setAutoStatus(mode, R.string.auto_vpn_status_checking_active)
        return try {
            val result = checkActiveProfileResult(profileId, mode, urlTester, periodicRetry, verifyTrafficPath)
            if (!result.bad && checkGeneration == activeProfileCheckGeneration.get() &&
                isSameStartedAutoProfile(profileId, mode)
            ) {
                failures = 0
                setAutoReadyStatus(mode)
                DiagnosticLog.event(
                    "auto-vpn",
                    "active-check-status-cleared reason=successful-attempt profile=$profileId mode=$mode",
                )
            }
            result
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            DiagnosticLog.error("auto-vpn", "active-profile-check-failed profile=$profileId mode=$mode", e)
            if (!periodicRetry && isSameStartedAutoProfile(profileId, mode)) {
                setActiveFailureStatus(
                    mode,
                    lastCheckedPingMillis?.takeIf { it > 0 },
                    (failures + 1).coerceAtLeast(1).coerceAtMost(failuresBeforeSwitch()),
                    failuresBeforeSwitch(),
                )
            }
            ActiveCheckResult(true, lastCheckedPingMillis)
        }
    }

    private suspend fun checkActiveProfileResult(
        profileId: Long,
        mode: Int,
        urlTester: () -> Int,
        periodicRetry: Boolean,
        verifyTrafficPath: Boolean,
    ): ActiveCheckResult {
        val ping = testStartedConnection(
            profileId,
            mode,
            urlTester,
            recordFailure = false,
            scoreEvent = AutoProfileScoreStore.Event.PERIODIC,
            scoreRetry = periodicRetry,
        )
        val pingFailed = ping == null || ping > badPingMillis()
        val traffic = if (verifyTrafficPath || pingFailed) {
            verifyRealTrafficPath(profileId, mode, "active-check")
        } else {
            null
        }
        var exitCountryRejected = false
        if (requiresExitCountryCheck(mode)) {
            val profile = SagerDatabase.proxyDao.getById(profileId) ?: return ActiveCheckResult(true)
            if (hasExcludedGlobeMarker(profile)) {
                AutoProfileScoreStore.recordFailure(profile, mode, AutoProfileScoreStore.Event.PERIODIC)
                exitCountryRejected = true
            } else if (!verifyActiveExitCountry(profile, mode, "active-check", AutoProfileScoreStore.Event.PERIODIC)) {
                if (!isSameStartedAutoProfile(profileId, mode)) return ActiveCheckResult(false)
                exitCountryRejected = true
            }
        }

        if (verifyTrafficPath || pingFailed) {
            if (traffic == null) {
                if (!periodicRetry) {
                    setActiveFailureStatus(
                        mode,
                        ping,
                        (failures + 1).coerceAtLeast(1).coerceAtMost(failuresBeforeSwitch()),
                        failuresBeforeSwitch(),
                    )
                }
                return ActiveCheckResult(true, ping, exitCountryRejected)
            }
            recordActiveTrafficSuccess(
                profileId,
                mode,
                traffic,
                scoreEvent = AutoProfileScoreStore.Event.PERIODIC,
                scoreRecovery = pingFailed,
            )
            if (exitCountryRejected || (ping != null && ping > badPingMillis())) {
                if (!periodicRetry) {
                    setActiveFailureStatus(
                        mode,
                        ping,
                        (failures + 1).coerceAtLeast(1).coerceAtMost(failuresBeforeSwitch()),
                        failuresBeforeSwitch(),
                    )
                }
                return ActiveCheckResult(true, ping, exitCountryRejected)
            }
            return ActiveCheckResult(false, ping ?: traffic.latencyMillis, exitCountryRejected)
        }
        if (ping == null || exitCountryRejected) {
            if (!periodicRetry) {
                setActiveFailureStatus(
                    mode,
                    ping,
                    (failures + 1).coerceAtLeast(1).coerceAtMost(failuresBeforeSwitch()),
                    failuresBeforeSwitch(),
                )
            }
            return ActiveCheckResult(true, ping, exitCountryRejected)
        }
        return ActiveCheckResult(false)
    }

    private suspend fun verifyRealTrafficPath(
        profileId: Long,
        mode: Int,
        source: String,
    ): ActiveVpnTrafficProbe.Result? {
        if (!isSameStartedAutoProfile(profileId, mode)) return null
        val proxy = ServiceAvailabilityProbe.activeProfileProxy() ?: run {
            DiagnosticLog.warn(
                "auto-vpn",
                "active-traffic-probe-no-tunnel-route profile=$profileId mode=$mode source=$source",
            )
            return null
        }
        val downloadEnabled = DataStore.autoVpnTrafficDownloadEnabled
        val first = ActiveVpnTrafficProbe.run(DataStore.connectionTestURL, proxy, downloadEnabled)
        val firstSlow = recordTrafficSpeedSample(profileId, first)
        logActiveTrafficProbe(profileId, mode, source, 1, first, firstSlow)
        if (first.hasTraffic && (!downloadEnabled || (first.download.complete && !firstSlow))) return first
        if (!downloadEnabled) return first.takeIf { it.hasTraffic }

        delay(TRAFFIC_CONFIRM_DELAY_MS)
        if (!isSameStartedAutoProfile(profileId, mode)) return first.takeIf { it.hasTraffic }
        val second = ActiveVpnTrafficProbe.run(DataStore.connectionTestURL, proxy, downloadEnabled)
        val secondSlow = recordTrafficSpeedSample(profileId, second)
        logActiveTrafficProbe(profileId, mode, source, 2, second, secondSlow)
        return when {
            second.download.complete -> second
            first.download.complete -> first
            else -> null
        }
    }

    private fun recordTrafficSpeedSample(
        profileId: Long,
        result: ActiveVpnTrafficProbe.Result,
    ): Boolean {
        val download = result.download
        if (download.expectedBytes == 0L) return false
        val speed = download.bytesPerSecond ?: return true
        if (!download.complete) return true
        return synchronized(activeTrafficSpeedLock) {
            val baseline = activeTrafficSpeedBaselines[profileId]
            if (baseline == null) {
                activeTrafficSpeedBaselines[profileId] = speed
                false
            } else {
                if (speed > baseline) activeTrafficSpeedBaselines[profileId] = speed
                speed < baseline / 5L
            }
        }
    }

    private fun logActiveTrafficProbe(
        profileId: Long,
        mode: Int,
        source: String,
        attempt: Int,
        result: ActiveVpnTrafficProbe.Result,
        lowSpeedSignal: Boolean,
    ) {
        val endpoints = result.endpoints.joinToString(",") {
            "${it.host}:${it.responseCode ?: it.error ?: "error"}/${it.elapsedMillis}ms/${it.success}"
        }
        val transfer = result.download
        DiagnosticLog.event(
            "auto-vpn",
            "active-traffic-probe source=$source mode=$mode profile=$profileId attempt=$attempt " +
                    "reachable=${result.hasTraffic} endpoints=[$endpoints] " +
                    "download=${transfer.downloadedBytes}/${transfer.expectedBytes} " +
                    "downloadError=${transfer.error ?: "none"} " +
                    "ttfbMs=${transfer.firstByteMillis ?: -1} elapsedMs=${transfer.elapsedMillis} " +
                    "bytesPerSecond=${transfer.bytesPerSecond ?: -1} complete=${transfer.complete} " +
                    "lowSpeedSignal=$lowSpeedSignal",
        )
    }

    private suspend fun recordActiveTrafficSuccess(
        profileId: Long,
        mode: Int,
        result: ActiveVpnTrafficProbe.Result,
        scoreEvent: AutoProfileScoreStore.Event = AutoProfileScoreStore.Event.POST_CONNECT,
        scoreRecovery: Boolean = false,
    ) {
        val profile = SagerDatabase.proxyDao.getById(profileId) ?: return
        if (profile.status == 3) {
            result.latencyMillis?.let {
                profile.ping = it
                recordLastPing(it)
            }
        }
        profile.status = 1
        profile.error = null
        saveProfileTestResultSafely(profile, "active-traffic-confirmed")
        if (scoreRecovery) {
            AutoProfileScoreStore.recordSuccess(profile, mode, scoreEvent, afterFailure = true)
        }
        val workingPing = profile.ping.takeIf { it > 0 } ?: result.latencyMillis ?: 1
        rememberWorkingResults(mode, listOf(CandidateResult(profile, workingPing)))
        notifyAutoStateChanged()
    }

    private suspend fun recordActiveTrafficFailure(
        profileId: Long,
        mode: Int,
        scoreEvent: AutoProfileScoreStore.Event,
        source: String,
    ) {
        val profile = SagerDatabase.proxyDao.getById(profileId) ?: return
        profile.status = 3
        profile.error = "Active VPN traffic probe failed"
        saveProfileTestResultSafely(profile, "active-traffic-failed-$source")
        AutoProfileScoreStore.recordFailure(profile, mode, scoreEvent)
    }

    private suspend fun testStartedConnection(
        profileId: Long,
        mode: Int,
        urlTester: () -> Int,
        recordFailure: Boolean = true,
        scoreEvent: AutoProfileScoreStore.Event = AutoProfileScoreStore.Event.PERIODIC,
        scoreRetry: Boolean = false,
    ): Int? {
        val profile = SagerDatabase.proxyDao.getById(profileId) ?: return null
        return try {
            val ping = urlTester()
            recordLastPing(ping)
            profile.ping = ping
            profile.status = 1
            profile.error = null
            saveProfileTestResultSafely(profile, "active-success")
            if (ping <= badPingMillis()) {
                AutoProfileScoreStore.recordSuccess(profile, mode, scoreEvent, scoreRetry)
            } else {
                AutoProfileScoreStore.recordFailure(profile, mode, scoreEvent)
                recordProfileFailure(profile, quarantine = false)
            }
            rememberWorkingResults(mode, listOf(CandidateResult(profile, ping)))
            ping
        } catch (e: Exception) {
            Logs.w(e)
            recordLastPing(null)
            profile.status = 3
            profile.error = e.readableMessage
            saveProfileTestResultSafely(profile, "active-failed")
            if (shouldCountScoreFailure(e)) {
                AutoProfileScoreStore.recordFailure(profile, mode, scoreEvent)
            }
            if (recordFailure) recordProfileFailure(profile, quarantine = true)
            null
        }
    }

    private suspend fun verifyConnectedExitCountry(profileId: Long, mode: Int): Boolean {
        val profile = SagerDatabase.proxyDao.getById(profileId) ?: return false
        if (requiresExitCountryCheck(mode) && hasExcludedGlobeMarker(profile)) return false
        return verifyActiveExitCountry(profile, mode, "post-connect", AutoProfileScoreStore.Event.POST_CONNECT)
    }

    private suspend fun verifyActiveExitCountry(
        profile: ProxyEntity,
        mode: Int,
        source: String,
        scoreEvent: AutoProfileScoreStore.Event,
    ): Boolean = exitCountryCheckMutex.withLock {
        val profileId = profile.id
        if (!isSameStartedAutoProfile(profileId, mode)) return@withLock false

        val geoInfo = queryProfileExitGeoInfo(profile, mode)
        val countryCode = geoInfo?.countryCode
        recordLastExitCountry(profileId, geoInfo?.countryName)
        notifyAutoStateChanged()
        DiagnosticLog.event(
            "auto-vpn",
            "exit-country-check source=$source mode=$mode profile=$profileId " +
                    "countryName=${geoInfo?.countryName ?: "unknown"} country=${countryCode ?: "unknown"}",
        )
        if (!requiresExitCountryCheck(mode) || (countryCode != null && countryCode != "RU")) {
            clearPendingExitCountryConfirmation(profileId, mode)
            forgetExitCountryRejectedProfile(profile, "$source-valid")
            return@withLock true
        }

        scheduleExitCountryConfirmation(profile, mode, source, scoreEvent, countryCode)
        DiagnosticLog.warn(
            "auto-vpn",
            "exit-country-awaiting-confirmation source=$source profile=$profileId mode=$mode " +
                    "country=${countryCode ?: "unknown"} retryAfterMs=$EXIT_COUNTRY_CONFIRMATION_DELAY_MS",
        )
        true
    }

    private fun scheduleExitCountryConfirmation(
        profile: ProxyEntity,
        mode: Int,
        source: String,
        scoreEvent: AutoProfileScoreStore.Event,
        firstCountryCode: String?,
    ) {
        val safeMode = sanitizeMode(mode)
        synchronized(exitCountryConfirmationLock) {
            val current = pendingExitCountryConfirmation
            if (exitCountryConfirmationJob?.isActive == true && current != null &&
                current.profile.id == profile.id && current.mode == safeMode
            ) return

            exitCountryConfirmationJob?.cancel()
            pendingExitCountryConfirmation = PendingExitCountryConfirmation(
                profile = profile,
                mode = safeMode,
                countryCode = firstCountryCode,
                source = source,
                scoreEvent = scoreEvent,
            )
            exitCountryConfirmationJob = runOnDefaultDispatcher {
                try {
                    while (isActive && isSameStartedAutoProfile(profile.id, safeMode)) {
                        delay(EXIT_COUNTRY_CONFIRMATION_DELAY_MS)
                        if (!isSameStartedAutoProfile(profile.id, safeMode)) return@runOnDefaultDispatcher

                        val pending = synchronized(exitCountryConfirmationLock) {
                            pendingExitCountryConfirmation?.takeIf {
                                it.profile.id == profile.id && it.mode == safeMode
                            }
                        } ?: return@runOnDefaultDispatcher

                        if (!hasDirectInternet()) {
                            DiagnosticLog.event(
                                "auto-vpn",
                                "exit-country-confirmation-deferred profile=${profile.id} mode=$safeMode " +
                                        "reason=direct-network-unavailable retryAfterMs=$EXIT_COUNTRY_CONFIRMATION_DELAY_MS",
                            )
                            continue
                        }

                        val confirmedInfo = exitCountryCheckMutex.withLock {
                            if (!isSameStartedAutoProfile(profile.id, safeMode)) return@withLock null
                            queryProfileExitGeoInfo(profile, safeMode)
                        }
                        if (!isSameStartedAutoProfile(profile.id, safeMode)) return@runOnDefaultDispatcher
                        if (!hasDirectInternet()) {
                            DiagnosticLog.event(
                                "auto-vpn",
                                "exit-country-confirmation-deferred profile=${profile.id} mode=$safeMode " +
                                        "reason=direct-network-lost-during-check retryAfterMs=$EXIT_COUNTRY_CONFIRMATION_DELAY_MS",
                            )
                            continue
                        }

                        val confirmedCode = confirmedInfo?.countryCode
                        recordLastExitCountry(profile.id, confirmedInfo?.countryName)
                        notifyAutoStateChanged()
                        DiagnosticLog.event(
                            "auto-vpn",
                            "exit-country-confirmation-result profile=${profile.id} mode=$safeMode " +
                                    "first=${pending.countryCode ?: "unknown"} " +
                                    "second=${confirmedCode ?: "unknown"}",
                        )

                        if (confirmedCode != null && confirmedCode != "RU") {
                            forgetExitCountryRejectedProfile(profile, "${pending.source}-confirmation-valid")
                            return@runOnDefaultDispatcher
                        }

                        if (confirmedCode != pending.countryCode) {
                            synchronized(exitCountryConfirmationLock) {
                                pendingExitCountryConfirmation?.takeIf {
                                    it.profile.id == profile.id && it.mode == safeMode
                                }?.countryCode = confirmedCode
                            }
                            DiagnosticLog.warn(
                                "auto-vpn",
                                "exit-country-confirmation-changed profile=${profile.id} mode=$safeMode " +
                                        "nextCheckAfterMs=$EXIT_COUNTRY_CONFIRMATION_DELAY_MS",
                            )
                            continue
                        }

                        rememberExitCountryRejectedProfiles(listOf(profile.id), "${pending.source}-confirmed")
                        AutoProfileScoreStore.recordFailure(profile, safeMode, pending.scoreEvent)
                        profile.status = 3
                        profile.error = if (confirmedCode == "RU") "Exit country is Russia" else "Exit country is unknown"
                        saveProfileTestResultSafely(profile, "exit-country-rejected-confirmed")
                        DiagnosticLog.critical(
                            "auto-vpn",
                            "exit-country-rejected-confirmed source=${pending.source} profile=${profile.id} " +
                                    "mode=$safeMode country=${confirmedCode ?: "unknown"}",
                        )
                        confirmActiveProfileFailure(profile.id, safeMode, "exit-country-confirmed")
                        return@runOnDefaultDispatcher
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    DiagnosticLog.error(
                        "auto-vpn",
                        "exit-country-confirmation-failed profile=${profile.id} mode=$safeMode",
                        e,
                    )
                } finally {
                    val currentJob = currentCoroutineContext()[Job]
                    synchronized(exitCountryConfirmationLock) {
                        if (exitCountryConfirmationJob === currentJob) {
                            exitCountryConfirmationJob = null
                            pendingExitCountryConfirmation = null
                        }
                    }
                }
            }
        }
    }

    private fun clearPendingExitCountryConfirmation(profileId: Long? = null, mode: Int? = null) {
        synchronized(exitCountryConfirmationLock) {
            val pending = pendingExitCountryConfirmation ?: return
            if (profileId != null && pending.profile.id != profileId) return
            if (mode != null && pending.mode != sanitizeMode(mode)) return
            exitCountryConfirmationJob?.cancel()
            exitCountryConfirmationJob = null
            pendingExitCountryConfirmation = null
        }
    }

    private suspend fun queryProfileExitGeoInfo(
        profile: ProxyEntity,
        mode: Int,
    ): ExitIpGeoChecker.GeoInfo? {
        val proxy = ServiceAvailabilityProbe.activeProfileProxy()
        if (proxy == null) {
            DiagnosticLog.warn(
                "auto-vpn",
                "active-exit-geoip-no-tunnel-route mode=$mode profile=${profile.id}",
            )
            return null
        }
        return try {
            ExitIpGeoChecker.geoInfo(proxy)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            DiagnosticLog.warn(
                "auto-vpn",
                "active-exit-geoip-failed mode=$mode profile=${profile.id}",
                e,
            )
            null
        }
    }

    private suspend fun switchAfterPostConnectFailures(profileId: Long, mode: Int) {
        switchMutex.withLock {
            val safeMode = sanitizeMode(mode)
            if (!isSameStartedAutoProfile(profileId, safeMode)) return@withLock
            markFailed(profileId, countFailure = false)
            failures = 0
            setAutoStatus(safeMode, R.string.auto_vpn_status_selecting)
            val best = findBestCandidate(
                excludedIds = automaticExcludedIds(safeMode, setOf(profileId)),
                forceTest = false,
                mode = safeMode,
            )
            if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode || best == null) {
                setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
                abandonPendingAutoTransitionIfStopped()
                return@withLock
            }
            switchToVerified(
                    initial = best,
                    mode = safeMode,
                    excludedIds = automaticExcludedIds(safeMode, setOf(profileId)),
                    forceTest = true,
                    startIfStopped = false,
                    feedback = false,
                    recordManualCycle = false,
                    switchReason = "post-connect-verification-failed",
                )
        }
    }

    private fun verifyCurrentAndSwitchIfBad(
        countFailure: Boolean,
        ignoreSwitchCooldown: Boolean,
        feedback: Boolean,
    ) {
        runOnDefaultDispatcher {
            verifyCurrentAndSwitchIfBadLocked(countFailure, ignoreSwitchCooldown, feedback)
        }
    }

    private suspend fun verifyCurrentAndSwitchIfBadLocked(
        countFailure: Boolean,
        ignoreSwitchCooldown: Boolean,
        feedback: Boolean,
    ) {
        switchMutex.withLock {
            if (!DataStore.autoVpnEnabled || !SagerNet.started || DataStore.startedProfile <= 0L) return@withLock
            val badCurrent = checkCurrentProfileBad()
            if (!badCurrent) {
                failures = 0
                return@withLock
            }

            if (countFailure) {
                failures++
                val mode = sanitizeMode(DataStore.autoVpnMode)
                setActiveFailureStatus(mode, null, failures, failuresBeforeSwitch())
                if (failures < failuresBeforeSwitch()) return@withLock
            }
            failures = 0

            val now = System.currentTimeMillis()
            if (!ignoreSwitchCooldown && now - lastSwitchAt < switchCooldownMillis()) {
                return@withLock
            }

            val mode = sanitizeMode(DataStore.autoVpnMode)
            val best = findBestCandidate(automaticExcludedIds(mode, currentProfileIds()), forceTest = true, mode = mode)
            if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != mode || best == null) {
                setAutoStatus(mode, R.string.auto_vpn_status_no_candidate)
                abandonPendingAutoTransitionIfStopped()
                return@withLock
            }
            switchToVerified(
                    initial = best,
                    mode = mode,
                    excludedIds = automaticExcludedIds(mode, currentProfileIds()),
                    forceTest = true,
                    startIfStopped = false,
                    feedback = feedback,
                    recordManualCycle = false,
                    switchReason = "active-profile-check-failed",
                )
        }
    }

    private suspend fun checkCurrentProfileBad(): Boolean {
        val current = SagerDatabase.proxyDao.getById(DataStore.startedProfile) ?: return true
        val mode = sanitizeMode(DataStore.autoVpnMode)
        if (!profileAllowed(current, mode)) {
            markFailed(current.id)
            return true
        }

        repeat(CURRENT_VERIFY_ATTEMPTS) { attempt ->
            val result = testProfile(
                current,
                mode,
                markFailure = false,
                scoreEvent = AutoProfileScoreStore.Event.PERIODIC,
                scoreRetry = attempt > 0,
            )
            if (result != null && result.ping <= badPingMillis()) {
                rememberWorkingResults(mode, listOf(result))
                setAutoReadyStatus(mode)
                return false
            }
            if (attempt < CURRENT_VERIFY_ATTEMPTS - 1) {
                delay(CURRENT_VERIFY_RETRY_DELAY_MS)
            }
        }

        markFailed(current.id, countFailure = false)
        return true
    }

    private suspend fun findBestCandidate(
        excludedIds: Set<Long>,
        forceTest: Boolean,
        mode: Int,
    ): CandidateResult? {
        val safeMode = sanitizeMode(mode)
        if (!pauseVpnForDirectTests("candidate-search")) return null
        val groupIds = enabledGroupIds()
        if (groupIds.isEmpty()) return null

        val poolCandidates = reserveProfileIds(safeMode)
            .mapNotNull { SagerDatabase.proxyDao.getById(it) }
            .filter { it.id !in excludedIds && profileAllowed(it, safeMode) && !isMarkedFailed(it.id) }

        val cachedPool = poolCandidates.mapNotNull(::cachedWorkingResult)
        if (cachedPool.isNotEmpty()) {
            return chooseBestCandidate(cachedPool, safeMode)
        }

        if (!forceTest && poolCandidates.isNotEmpty()) {
            val first = poolCandidates.minWithOrNull(candidateComparator(safeMode)) ?: poolCandidates.first()
            return CandidateResult(first, first.ping)
        }
        if (poolCandidates.isNotEmpty()) {
            // The caller will perform the direct pre-switch test. In a detached
            // process the in-memory cache is intentionally empty, so do not skip
            // a valid persisted reserve and rescan every profile unnecessarily.
            val first = poolCandidates.minWithOrNull(candidateComparator(safeMode)) ?: poolCandidates.first()
            return CandidateResult(first, first.ping)
        }
        return findBestCandidateFromScan(groupIds, excludedIds, safeMode, countProgress = false)
    }

    private suspend fun findBestCandidateFromScan(
        groupIds: List<Long>,
        excludedIds: Set<Long>,
        mode: Int,
        countProgress: Boolean,
    ): CandidateResult? {
        val safeMode = sanitizeMode(mode)
        val snapshot = autoScanProfiles(groupIds)
        if (snapshot.isEmpty()) return null

        // Once the reserve is empty, rebuild it with the same target and timeout used
        // for the initial Auto start. This happens only after the VPN is paused, so no
        // candidate is measured through the active tunnel.
        val ownsProgress = !countProgress && beginOnDemandSearchProgress(safeMode, snapshot.size)
        val trackProgress = countProgress || ownsProgress
        setAutoStatus(safeMode, R.string.auto_vpn_status_testing_more)

        try {
            val working = scanInitialWorkingProfiles(
                groupIds = groupIds,
                excludedIds = excludedIds,
                mode = safeMode,
                target = reserveTarget(),
                countProgress = trackProgress,
            )
            return chooseBestCandidate(working, safeMode)
        } finally {
            if (ownsProgress && autoScanJob?.isActive != true) {
                finishAutoProgress()
            }
        }
    }

    private fun chooseBestCandidate(results: List<CandidateResult>, mode: Int): CandidateResult? {
        if (results.isEmpty()) return null
        return results.minWithOrNull(candidateResultComparator())
    }

    private fun sortCandidatesByScore(results: List<CandidateResult>, mode: Int): List<CandidateResult> {
        if (results.isEmpty()) return emptyList()
        return results.sortedWith(candidateResultComparator())
    }

    private fun candidateComparator(mode: Int): Comparator<ProxyEntity> {
        return compareBy<ProxyEntity> { if (it.ping > 0) it.ping else Int.MAX_VALUE }
            .thenBy { it.userOrder }
            .thenBy { it.id }
    }

    private fun candidateResultComparator(): Comparator<CandidateResult> =
        compareBy<CandidateResult> { if (it.ping > 0) it.ping else Int.MAX_VALUE }
            .thenBy { it.profile.userOrder }
            .thenBy { it.profile.id }

    private fun sortProfileIdsByScore(profileIds: Collection<Long>, mode: Int): List<Long> {
        if (profileIds.isEmpty()) return emptyList()
        return profileIds
            .mapNotNull { profileId ->
                val profile = SagerDatabase.proxyDao.getById(profileId) ?: return@mapNotNull null
                val ping = workingCache[profileId]?.ping ?: profile.ping
                Triple(profileId, ping, profile.userOrder)
            }
            .sortedWith(
                compareBy<Triple<Long, Int, Long>> { if (it.second > 0) it.second else Int.MAX_VALUE }
                    .thenBy { it.third }
                    .thenBy { it.first },
            )
            .map { it.first }
    }

    private fun selectionReason(profile: ProxyEntity, mode: Int): String {
        val ping = workingCache[profile.id]?.ping ?: profile.ping
        return app.getString(R.string.auto_vpn_selection_reason_ping, ping.coerceAtLeast(0))
    }

    private fun recordProfileFailure(profile: ProxyEntity, quarantine: Boolean) {
        if (quarantine) {
            failedUntil[profile.id] = System.currentTimeMillis() + quarantineMillis()
        }
    }

    private fun markManualSkipped(profileId: Long, mode: Int) {
        if (profileId <= 0L) return
        SagerDatabase.proxyDao.getById(profileId)?.let {
            AutoProfileScoreStore.recordManualSwitch(it, mode)
        }
        failedUntil[profileId] = System.currentTimeMillis() + quarantineMillis()
        if (sanitizeMode(mode) == MODE_GEMINI) {
            rememberGeminiRejectedProfiles(listOf(profileId))
        }
    }

    private fun rememberWorkingResults(mode: Int, results: List<CandidateResult>) {
        if (results.isEmpty()) return
        val safeMode = sanitizeMode(mode)
        val now = System.currentTimeMillis()
        results.forEach {
            workingCache[it.profile.id] = CachedWorking(it.ping, now)
        }
        val activeIds = currentProfileIds().toSet()
        val manualIds = manualCycleIds[safeMode].orEmpty()
        val existingIds = LinkedHashSet(reserveProfileIds(safeMode))
        sortCandidatesByScore(results, safeMode).forEach {
            val id = it.profile.id
            if (id > 0L &&
                id !in activeIds &&
                id !in manualIds &&
                !isMarkedFailed(id) &&
                profileAllowed(it.profile, safeMode)
            ) {
                existingIds += id
            }
        }
        synchronized(workingPools) {
            workingPools[safeMode] = sortProfileIdsByScore(existingIds, safeMode)
        }
        updateAutoReserve(safeMode)
    }

    private fun cachedWorkingResult(profile: ProxyEntity): CandidateResult? {
        val cached = workingCache[profile.id] ?: return null
        if (System.currentTimeMillis() - cached.at > RECENT_WORKING_TTL_MS) {
            workingCache.remove(profile.id)
            return null
        }
        return CandidateResult(profile, cached.ping)
    }

    private fun hasFreshWorkingCache(profileId: Long): Boolean {
        if (profileId <= 0L) return false
        val cached = workingCache[profileId] ?: return false
        if (System.currentTimeMillis() - cached.at <= RECENT_WORKING_TTL_MS) return true
        workingCache.remove(profileId)
        return false
    }

    private fun enabledGroupIds(): List<Long> {
        return SagerDatabase.groupDao.allGroups().filter {
            it.id > 0L &&
                    DataStore.isGroupEnabled(it.id) &&
                    (!it.ungrouped || SagerDatabase.proxyDao.countByGroup(it.id) > 0L)
        }.map { it.id }
    }

    private fun autoScanProfiles(groupIds: List<Long>): List<ProxyEntity> {
        return when (DataStore.allGroupOrder) {
            GroupOrder.BY_NAME -> SagerDatabase.proxyDao.getAutoScanProfilesByName(groupIds)
            GroupOrder.BY_DELAY -> SagerDatabase.proxyDao.getAutoScanProfilesByDelay(groupIds)
            else -> SagerDatabase.proxyDao.getAutoScanProfilesByOrigin(groupIds)
        }
    }

    private fun autoTestThrottle(): AutoTestThrottle {
        return AutoTestThrottle(
            quickFailureLimit = DataStore.connectionTestQuickFailureCount.coerceAtLeast(1),
            quickFailureMillis = DataStore.connectionTestQuickFailureMillis.coerceAtLeast(1),
            quickFailurePauseMillis = DataStore.connectionTestQuickFailurePauseSeconds.coerceAtLeast(1) * 1000L,
            scheduledBreakAfterCount = DataStore.connectionTestBreakAfterCount.coerceAtLeast(0),
            scheduledBreakMillis = DataStore.connectionTestBreakSeconds.coerceAtLeast(0) * 1000L,
        )
    }

    private suspend fun testCandidates(
        candidates: List<ProxyEntity>,
        mode: Int,
        countProgress: Boolean,
        throttle: AutoTestThrottle? = null,
        onWorkingResult: ((CandidateResult) -> Unit)? = null,
    ): List<CandidateResult> = coroutineScope {
        val parallel = DataStore.connectionTestParallel.coerceAtLeast(1)
        val semaphore = Semaphore(parallel)
        val throttleLock = Any()
        val outcomes = ConcurrentLinkedQueue<CandidateOutcome>()
        val jobs = ArrayList<Job>(candidates.size)
        for (profile in candidates) {
            jobs += launch(start = CoroutineStart.LAZY) {
                semaphore.withPermit {
                    throttle?.waitForPause()
                    val outcome = testProfileOutcome(profile, mode)
                    if (!isActive) return@withPermit
                    outcome.result?.let { onWorkingResult?.invoke(it) }
                    if (throttle != null) {
                        synchronized(throttleLock) {
                            throttle.record(outcome)
                        }
                    }
                    if (countProgress) {
                        recordAutoTestResult(outcome.result != null)
                    }
                    outcomes += outcome
                }
            }
        }
        jobs.forEach { it.start() }
        jobs.joinAll()
        outcomes.mapNotNull { it.result }
    }

    private suspend fun testProfile(
        profile: ProxyEntity,
        mode: Int,
        markFailure: Boolean = true,
        verifyExitCountry: Boolean = false,
        scoreEvent: AutoProfileScoreStore.Event = AutoProfileScoreStore.Event.CANDIDATE,
        scoreRetry: Boolean = false,
    ): CandidateResult? = testProfileOutcome(
        profile,
        mode,
        markFailure,
        verifyExitCountry,
        scoreEvent,
        scoreRetry,
    ).result

    private suspend fun testProfileOutcome(
        profile: ProxyEntity,
        mode: Int,
        markFailure: Boolean = true,
        verifyExitCountry: Boolean = false,
        scoreEvent: AutoProfileScoreStore.Event = AutoProfileScoreStore.Event.CANDIDATE,
        scoreRetry: Boolean = false,
    ): CandidateOutcome {
        val timeout = DataStore.connectionTestTimeoutSeconds.coerceAtLeast(1) * 1000
        val link = testUrlForMode()
        val startedAt = System.currentTimeMillis()
        profile.status = 0
        profile.error = null
        DiagnosticLog.event(
            "auto-vpn",
            "candidate-test-start mode=$mode markFailure=$markFailure " +
                    ProfileTestRunner.profileDiagnosticSummary(profile)
        )

        return try {
            if (ProfileTestRunner.useBrowserForwarder(profile)) {
                DiagnosticLog.event(
                    "auto-vpn",
                    "candidate-test-skip-browser-forwarder mode=$mode " +
                            ProfileTestRunner.profileDiagnosticSummary(profile)
                )
                CandidateOutcome(null, System.currentTimeMillis() - startedAt, null)
            } else {
                val instance = V2RayTestInstance(
                    profile,
                    link,
                    timeout,
                    protectPath = protectPathForTests(),
                    enableHttpProxy = true,
                )
                val ping = try {
                    val result = instance.doTestHttp204(link, timeout)
                    if (verifyExitCountry && requiresExitCountryCheck(mode)) {
                        val countryCode = instance.exitIpCountryCode()
                        DiagnosticLog.event(
                            "auto-vpn",
                            "candidate-exit-country mode=$mode profile=${profile.id} " +
                                    "country=${countryCode ?: "unknown"}",
                        )
                        if (countryCode == null || countryCode == "RU") {
                            rememberExitCountryRejectedProfiles(listOf(profile.id), "candidate-precheck")
                            throw IOException(
                                if (countryCode == "RU") "Exit country is Russia" else "Exit country is unknown"
                            )
                        }
                    }
                    result
                } finally {
                    instance.close()
                }
                recordLastPing(ping)
                profile.ping = ping
                profile.status = 1
                saveProfileTestResultSafely(profile, "candidate-success")
                if (scoreEvent == AutoProfileScoreStore.Event.PERIODIC && ping > badPingMillis()) {
                    AutoProfileScoreStore.recordFailure(profile, mode, scoreEvent)
                } else {
                    AutoProfileScoreStore.recordSuccess(profile, mode, scoreEvent, scoreRetry)
                }
                DiagnosticLog.event(
                    "auto-vpn",
                    "candidate-test-success mode=$mode elapsedMs=${System.currentTimeMillis() - startedAt} " +
                            ProfileTestRunner.profileDiagnosticSummary(profile)
                )
                CandidateOutcome(CandidateResult(profile, ping), System.currentTimeMillis() - startedAt, null)
            }
        } catch (e: CancellationException) {
            DiagnosticLog.event(
                "auto-vpn",
                "candidate-test-cancelled mode=$mode elapsedMs=${System.currentTimeMillis() - startedAt} " +
                        ProfileTestRunner.profileDiagnosticSummary(profile)
            )
            throw e
        } catch (e: Exception) {
            recordLastPing(null)
            profile.status = 3
            profile.error = e.readableMessage
            saveProfileTestResultSafely(profile, "candidate-failed")
            if (shouldCountScoreFailure(e)) {
                AutoProfileScoreStore.recordFailure(profile, mode, scoreEvent)
            } else {
                DiagnosticLog.event(
                    "auto-vpn",
                    "profile-score-failure-ignored-no-direct-network mode=$mode profile=${profile.id} " +
                            "event=${scoreEvent.name.lowercase()}",
                )
            }
            recordProfileFailure(profile, quarantine = markFailure)
            Logs.d("Auto VPN candidate failed: ${profile.displayName()} - ${e.readableMessage}")
            DiagnosticLog.event(
                "auto-vpn",
                "candidate-test-failed mode=$mode elapsedMs=${System.currentTimeMillis() - startedAt} " +
                        ProfileTestRunner.profileDiagnosticSummary(profile)
            )
            CandidateOutcome(null, System.currentTimeMillis() - startedAt, e)
        }
    }

    private suspend fun saveProfileTestResultSafely(profile: ProxyEntity, reason: String) {
        repeat(PROFILE_RESULT_DB_RETRY_COUNT) { attempt ->
            try {
                ProfileManager.updateProfileTestResult(profile)
                if (attempt > 0) {
                    DiagnosticLog.event(
                        "auto-vpn",
                        "profile-result-save-recovered reason=$reason attempt=${attempt + 1} " +
                                ProfileTestRunner.profileDiagnosticSummary(profile)
                    )
                }
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: SQLiteDatabaseLockedException) {
                DiagnosticLog.warn(
                    "auto-vpn",
                    "profile-result-save-locked reason=$reason attempt=${attempt + 1}/$PROFILE_RESULT_DB_RETRY_COUNT " +
                            ProfileTestRunner.profileDiagnosticSummary(profile),
                    e,
                )
                delay(PROFILE_RESULT_DB_RETRY_DELAY_MS * (attempt + 1))
            } catch (e: Exception) {
                Logs.w(e)
                DiagnosticLog.warn(
                    "auto-vpn",
                    "profile-result-save-failed reason=$reason " +
                            ProfileTestRunner.profileDiagnosticSummary(profile),
                    e,
                )
                return
            }
        }
        DiagnosticLog.warn(
            "auto-vpn",
            "profile-result-save-skipped-db-busy reason=$reason " +
                    ProfileTestRunner.profileDiagnosticSummary(profile)
        )
    }

    private fun testUrlForMode(): String {
        return DataStore.connectionTestURL
    }

    internal fun profileAllowed(profile: ProxyEntity, mode: Int): Boolean {
        if (mode != MODE_ANY && looksRussian(profile)) return false
        if (requiresExitCountryCheck(mode) &&
            (hasExcludedGlobeMarker(profile) || isExitCountryRejected(profile))
        ) return false
        return mode != MODE_GEMINI || !isGeminiRejected(profile)
    }

    private fun hasExcludedGlobeMarker(profile: ProxyEntity): Boolean {
        val displayName = profile.displayName()
        val cachedName = profile.profileNameCache
        return "🌐" in displayName || "🌐" in cachedName ||
                "🏳" in displayName || "🏳" in cachedName
    }

    private fun requiresExitCountryCheck(mode: Int): Boolean =
        mode == MODE_NON_RU || mode == MODE_GEMINI

    private fun rememberGeminiRejectedProfiles(profileIds: Collection<Long>) {
        if (profileIds.isEmpty()) return
        val keys = LinkedHashSet(DataStore.autoVpnGeminiRejectedKeys)
        profileIds
            .mapNotNull { SagerDatabase.proxyDao.getById(it) }
            .flatMap(::profileRejectKeys)
            .forEach(keys::add)

        while (keys.size > MAX_GEMINI_REJECTED_KEYS) {
            val iterator = keys.iterator()
            if (!iterator.hasNext()) break
            iterator.next()
            iterator.remove()
        }
        DataStore.autoVpnGeminiRejectedKeys = keys
    }

    private fun isGeminiRejected(profile: ProxyEntity): Boolean {
        val rejectedKeys = DataStore.autoVpnGeminiRejectedKeys
        return profileRejectKeys(profile).any(rejectedKeys::contains)
    }

    private fun rememberExitCountryRejectedProfiles(profileIds: Collection<Long>, reason: String) {
        if (profileIds.isEmpty()) return
        synchronized(exitCountryRejectedWriteLock) {
            val keys = LinkedHashSet(exitCountryRejectedKeys())
            val profiles = profileIds.mapNotNull { SagerDatabase.proxyDao.getById(it) }
            profiles.mapNotNull(::profileRejectKey).forEach(keys::add)
            DataStore.autoVpnExitCountryRejectedKeys = keys
            updateExitCountryRejectedCache(keys, bumpExitCountryRejectedRevision())
            DiagnosticLog.critical(
                "auto-vpn",
                "exit-country-rejected-persisted reason=$reason profiles=${profiles.map { it.id }} keys=${keys.size}",
            )
        }
    }

    private fun forgetExitCountryRejectedProfile(profile: ProxyEntity, reason: String) {
        val key = profileRejectKey(profile) ?: return
        synchronized(exitCountryRejectedWriteLock) {
            val keys = LinkedHashSet(exitCountryRejectedKeys())
            if (!keys.remove(key)) return
            DataStore.autoVpnExitCountryRejectedKeys = keys
            updateExitCountryRejectedCache(keys, bumpExitCountryRejectedRevision())
            DiagnosticLog.event(
                "auto-vpn",
                "exit-country-rejection-cleared reason=$reason profile=${profile.id} keys=${keys.size}",
            )
        }
    }

    private fun isExitCountryRejected(profile: ProxyEntity): Boolean {
        val rejected = exitCountryRejectedKeys()
        return profileRejectKeys(profile).any(rejected::contains)
    }

    private fun exitCountryRejectedKeys(): Set<String> {
        refreshExitCountryRejectedCacheIfNeeded()
        return cachedExitCountryRejectedKeys.orEmpty()
    }

    private fun refreshExitCountryRejectedCacheIfNeeded() {
        val revision = runCatching { DataStore.autoVpnExitCountryRejectedRevision }.getOrDefault(0L)
        if (cachedExitCountryRejectedRevision == revision && cachedExitCountryRejectedKeys != null) return
        synchronized(this) {
            val currentRevision = runCatching { DataStore.autoVpnExitCountryRejectedRevision }.getOrDefault(0L)
            if (cachedExitCountryRejectedRevision == currentRevision && cachedExitCountryRejectedKeys != null) return
            val hadRejectedProfiles = !cachedExitCountryRejectedKeys.isNullOrEmpty()
            val keys = runCatching { DataStore.autoVpnExitCountryRejectedKeys }
                .getOrElse {
                    Logs.w(it)
                    emptySet()
                }
            updateExitCountryRejectedCache(keys, currentRevision)
            if (hadRejectedProfiles && keys.isEmpty()) clearExitCountryTransientExclusions()
        }
    }

    private fun updateExitCountryRejectedCache(keys: Set<String>, revision: Long) {
        cachedExitCountryRejectedKeys = keys.toSet()
        cachedExitCountryRejectedRevision = revision
    }

    private fun bumpExitCountryRejectedRevision(): Long {
        val current = runCatching { DataStore.autoVpnExitCountryRejectedRevision }.getOrDefault(0L)
        val next = maxOf(System.currentTimeMillis(), current + 1L)
        DataStore.autoVpnExitCountryRejectedRevision = next
        return next
    }

    private fun clearExitCountryTransientExclusions() {
        workingCache.clear()
        listOf(MODE_NON_RU, MODE_GEMINI).forEach { mode ->
            manualCycleIds.remove(mode)
            synchronized(scannedIds) { scannedIds.remove(mode) }
            synchronized(workingPools) {
                workingPools.remove(mode)
                hydratedReserveModes.remove(mode)
            }
            when (mode) {
                MODE_NON_RU -> {
                    DataStore.autoVpnReserveIdsNonRu = ""
                    DataStore.autoVpnReserveCountNonRu = 0
                }
                MODE_GEMINI -> {
                    DataStore.autoVpnReserveIdsGemini = ""
                    DataStore.autoVpnReserveCountGemini = 0
                }
            }
        }
    }

    private fun profileRejectKeys(profile: ProxyEntity): List<String> {
        val keys = LinkedHashSet<String>()
        profileRejectKey(profile)?.let(keys::add)
        profile.displayName().trim().takeIf { it.isNotEmpty() }?.let { name ->
            // A subscription can refresh a link while preserving its exact profile name.
            keys += "name:$name"
        }
        return keys.toList()
    }

    private fun profileRejectKey(profile: ProxyEntity): String? {
        profile.importKeyCache.trim().takeIf { it.isNotEmpty() }?.let {
            return "link:$it"
        }
        return runCatching {
            val bean = profile.requireBean().applyDefaultValues()
            "bean:${profile.type}:${bean.serverAddress}:${bean.serverPort}:${bean.profileId}"
        }.getOrNull()?.takeIf { it.isNotBlank() } ?: "id:${profile.id}".takeIf { profile.id > 0L }
    }

    fun isKnownNonRussianProfile(profile: ProxyEntity, groupName: String = ""): Boolean {
        if (looksRussian(profile, groupName)) return false
        return hasKnownCountryMarker(listOf(profile.displayName(), profile.profileNameCache, groupName))
    }

    private fun looksRussian(profile: ProxyEntity, groupNameOverride: String? = null): Boolean {
        val groupName = groupNameOverride ?: runCatching {
            SagerDatabase.groupDao.getById(profile.groupId)?.displayName().orEmpty()
        }.getOrDefault("")
        val label = listOf(
            profile.displayName(),
            profile.profileNameCache,
            groupName,
        ).joinToString(" ").lowercase(Locale.ROOT)

        if (RUSSIA_FLAG in label) return true
        if (russianTokenRegex.containsMatchIn(label)) return true

        val host = serverHost(profile) ?: return false
        return isRussianHost(host)
    }

    private fun hasKnownCountryMarker(labels: List<String>): Boolean {
        val text = labels.joinToString(" ")
        if (text.isBlank()) return false
        val normalizedText = text.lowercase(Locale.ROOT).replace('ё', 'е')
        if (knownCountryNameRegex.containsMatchIn(normalizedText)) return true

        val uppercaseCodes = Regex("(?<![A-Za-z])([A-Z]{2})(?![A-Za-z])")
        if (uppercaseCodes.findAll(text).any {
                it.groupValues[1] in isoCountryCodes || it.groupValues[1] == "UK"
            }
        ) return true

        var index = 0
        while (index < text.length) {
            val first = Character.codePointAt(text, index)
            val nextIndex = index + Character.charCount(first)
            if (nextIndex >= text.length) break
            val second = Character.codePointAt(text, nextIndex)
            if (first !in 0x1F1E6..0x1F1FF || second !in 0x1F1E6..0x1F1FF) {
                index = nextIndex
                continue
            }
            val countryCode = buildString {
                append(('A'.code + first - 0x1F1E6).toChar())
                append(('A'.code + second - 0x1F1E6).toChar())
            }
            if (countryCode in isoCountryCodes && countryCode != "RU") return true
            index = nextIndex + Character.charCount(second)
        }
        return false
    }

    private fun serverHost(profile: ProxyEntity): String? {
        return runCatching {
            profile.requireBean().applyDefaultValues().serverAddress
        }.getOrNull()
            ?.takeIf { it.isNotBlank() }
            ?: profile.displayAddress()
                .substringBeforeLast(":")
                .removePrefix("[")
                .removeSuffix("]")
                .takeIf { it.isNotBlank() }
    }

    private fun isRussianHost(host: String): Boolean {
        val cleanHost = host.trim().removePrefix("[").removeSuffix("]").trimEnd('.')
        if (cleanHost.isBlank()) return false
        val lowerHost = cleanHost.lowercase(Locale.ROOT)
        val ascii = runCatching {
            IDN.toASCII(lowerHost, IDN.ALLOW_UNASSIGNED).lowercase(Locale.ROOT)
        }.getOrDefault(lowerHost)
        val unicode = runCatching {
            IDN.toUnicode(ascii).lowercase(Locale.ROOT)
        }.getOrDefault(lowerHost)

        if (ascii == "ru" || ascii.endsWith(".ru")) return true
        if (ascii == "su" || ascii.endsWith(".su")) return true
        if (ascii == "xn--p1ai" || ascii.endsWith(".xn--p1ai")) return true
        if (unicode == RUSSIA_CYRILLIC_TLD || unicode.endsWith(".$RUSSIA_CYRILLIC_TLD")) return true

        val ip = ipv4ToLong(lowerHost) ?: return false
        return russianIpv4Cidrs.any { it.contains(ip) }
    }

    private fun parseCidr(value: String): Ipv4Cidr? {
        val address = value.substringBefore("/")
        val prefix = value.substringAfter("/", "").toIntOrNull() ?: return null
        if (prefix !in 0..32) return null
        val ip = ipv4ToLong(address) ?: return null
        val mask = if (prefix == 0) 0L else (0xffffffffL shl (32 - prefix)) and 0xffffffffL
        return Ipv4Cidr(ip and mask, mask)
    }

    private fun ipv4ToLong(value: String): Long? {
        val parts = value.split(".")
        if (parts.size != 4) return null
        var result = 0L
        for (part in parts) {
            val octet = part.toIntOrNull() ?: return null
            if (octet !in 0..255) return null
            result = (result shl 8) or octet.toLong()
        }
        return result and 0xffffffffL
    }

    private fun protectPathForTests(): String {
        return if (
            DataStore.tunImplementation == TunImplementation.SYSTEM &&
            DataStore.serviceMode == Key.MODE_VPN &&
            SagerNet.started &&
            DataStore.startedProfile > 0L
        ) {
            SagerNet.deviceStorage.noBackupFilesDir.toString() + "/protect_path"
        } else {
            ""
        }
    }

    fun isDirectSearchActive(): Boolean = AutoVpnDirectSearchService.isSearchActive()

    private fun isVpnRunningForAuto(): Boolean {
        return SagerNet.started || DataStore.startedProfile > 0L
    }

    private suspend fun pauseVpnForDirectTests(reason: String): Boolean {
        if (!isVpnRunningForAuto()) return true

        val mode = sanitizeMode(DataStore.autoVpnMode)
        val profileId = DataStore.startedProfile.takeIf { it > 0L } ?: DataStore.selectedProxy
        DiagnosticLog.critical(
            "auto-vpn",
            "direct-test-handoff-start reason=$reason mode=$mode profile=$profileId ${DiagnosticLog.stateSummary()}"
        )
        pendingAutoServiceTransition = true
        DataStore.autoVpnServiceStartRequested = false
        if (!AutoVpnDirectSearchService.startCandidateSearch(app, mode, profileId, reason)) {
            pendingAutoServiceTransition = false
            return false
        }
        onMainDispatcher {
            SagerNet.stopService()
        }
        return false
    }

    private fun abandonPendingAutoTransitionIfStopped() {
        if (!isVpnRunningForAuto()) {
            pendingAutoServiceTransition = false
            DataStore.autoVpnServiceStartRequested = false
        }
    }

    private suspend fun switchToVerified(
        initial: CandidateResult,
        mode: Int,
        excludedIds: Set<Long>,
        forceTest: Boolean,
        startIfStopped: Boolean,
        feedback: Boolean,
        recordManualCycle: Boolean,
        switchReason: String = "automatic-selection",
    ): Boolean {
        val safeMode = sanitizeMode(mode)
        val precheckEnabled = DataStore.autoVpnPrecheckEnabled
        if ((precheckEnabled || requiresExitCountryCheck(safeMode)) &&
            !pauseVpnForDirectTests("candidate-precheck")
        ) return false
        val rejectedIds = LinkedHashSet<Long>()
        var candidate: CandidateResult? = initial

        while (candidate != null &&
            DataStore.autoVpnEnabled &&
            DataStore.autoVpnMode == safeMode
        ) {
            val profile = candidate.profile
            rejectedIds += profile.id

            if (!profileAllowed(profile, safeMode) || isMarkedFailed(profile.id)) {
                consumeReserve(profile.id, safeMode)
                workingCache.remove(profile.id)
                candidate = findBestCandidate(
                    automaticExcludedIds(safeMode, excludedIds + rejectedIds + currentProfileIds()),
                    forceTest = forceTest,
                    mode = safeMode,
                )
                continue
            }

            if (!precheckEnabled && !requiresExitCountryCheck(safeMode)) {
                setAutoStatus(safeMode, R.string.auto_vpn_status_selecting)
                DiagnosticLog.critical(
                    "auto-vpn",
                    "pre-switch-test-skipped mode=$safeMode " +
                            ProfileTestRunner.profileDiagnosticSummary(profile)
                )
                switchTo(
                    profile = profile,
                    startIfStopped = startIfStopped,
                    feedback = feedback,
                    recordManualCycle = recordManualCycle,
                    switchReason = switchReason,
                )
                return true
            }

            setAutoStatus(safeMode, R.string.auto_vpn_status_prechecking_candidate, profile.displayName())
            DiagnosticLog.critical(
                "auto-vpn",
                "pre-switch-test-start mode=$safeMode startIfStopped=$startIfStopped " +
                        ProfileTestRunner.profileDiagnosticSummary(profile)
            )

            val verified = testProfile(
                profile,
                safeMode,
                markFailure = true,
                verifyExitCountry = requiresExitCountryCheck(safeMode),
                scoreEvent = AutoProfileScoreStore.Event.PRE_SWITCH,
            )
            if (verified != null && verified.ping > 0) {
                rememberWorkingResults(safeMode, listOf(verified))
                DiagnosticLog.critical(
                    "auto-vpn",
                    "pre-switch-test-success mode=$safeMode ping=${verified.ping} " +
                            ProfileTestRunner.profileDiagnosticSummary(verified.profile)
                )
                switchTo(
                    profile = verified.profile,
                    startIfStopped = startIfStopped,
                    feedback = feedback,
                    recordManualCycle = recordManualCycle,
                    switchReason = switchReason,
                )
                return true
            }

            consumeReserve(profile.id, safeMode)
            workingCache.remove(profile.id)
            DiagnosticLog.warn(
                "auto-vpn",
                "pre-switch-test-rejected mode=$safeMode ping=${verified?.ping ?: -1} " +
                        ProfileTestRunner.profileDiagnosticSummary(profile)
            )

            candidate = findBestCandidate(
                automaticExcludedIds(safeMode, excludedIds + rejectedIds + currentProfileIds()),
                forceTest = true,
                mode = safeMode,
            )
        }

        if (DataStore.autoVpnEnabled && DataStore.autoVpnMode == safeMode) {
            setAutoStatus(safeMode, R.string.auto_vpn_status_no_candidate)
            abandonPendingAutoTransitionIfStopped()
        }
        return false
    }

    private suspend fun switchTo(
        profile: ProxyEntity,
        startIfStopped: Boolean,
        feedback: Boolean,
        recordManualCycle: Boolean,
        switchReason: String,
    ) {
        val previous = DataStore.selectedProxy
        consumeReserve(profile.id, sanitizeMode(DataStore.autoVpnMode))
        DiagnosticLog.critical(
            "auto-vpn",
            "switchTo previous=$previous startIfStopped=$startIfStopped feedback=$feedback " +
                    "manualCycle=$recordManualCycle trigger=$switchReason " +
                    "selection=${selectionReason(profile, sanitizeMode(DataStore.autoVpnMode))} " +
                    "started=${SagerNet.started} " +
                    ProfileTestRunner.profileDiagnosticSummary(profile)
        )
        if (previous == profile.id && SagerNet.started) {
            setAutoReadyStatus(DataStore.autoVpnMode)
            DiagnosticLog.critical("auto-vpn", "switchTo-same-profile previous=$previous")
            return
        }

        DataStore.selectedProxy = profile.id
        DataStore.selectedGroup = profile.groupId
        ProfileManager.postUpdate(previous)
        ProfileManager.postUpdate(profile)
        lastSwitchAt = System.currentTimeMillis()
        lastDecisionReason = "$switchReason: ${selectionReason(profile, sanitizeMode(DataStore.autoVpnMode))}"

        if (recordManualCycle) {
            manualCycleIds.getOrPut(sanitizeMode(DataStore.autoVpnMode)) { LinkedHashSet() } += profile.id
        }
        if (DataStore.autoVpnEnabled) {
            setAutoStatus(DataStore.autoVpnMode, R.string.auto_vpn_status_switched_to, profile.displayName())
        }
        if (feedback) {
            notifyUser(R.string.auto_vpn_switched_to, profile.displayName())
        }

        val willStartService = SagerNet.started || startIfStopped || pendingAutoServiceTransition
        if (willStartService) {
            pendingAutoServiceTransition = true
            if (!SagerNet.started) {
                DataStore.autoVpnServiceStartRequested = true
            }
        }
        onMainDispatcher {
            if (SagerNet.started) {
                SagerNet.reloadService()
            } else if (willStartService) {
                SagerNet.startService()
            }
        }
    }

    private fun markFailed(profileId: Long, countFailure: Boolean = true) {
        if (profileId <= 0L) return
        val profile = SagerDatabase.proxyDao.getById(profileId)
        if (profile != null && countFailure) recordProfileFailure(profile, quarantine = true)
        else failedUntil[profileId] = System.currentTimeMillis() + quarantineMillis()
    }

    private fun isMarkedFailed(profileId: Long): Boolean {
        val now = System.currentTimeMillis()
        val until = failedUntil[profileId]
        if (until != null) {
            if (until > now) return true
            failedUntil.remove(profileId)
        }
        return false
    }

    private fun profileIdForMode(mode: Int): Long {
        return when (mode) {
            MODE_NON_RU -> AUTO_NON_RU_PROFILE_ID
            MODE_GEMINI -> AUTO_GEMINI_PROFILE_ID
            else -> AUTO_PROFILE_ID
        }
    }

    private fun sanitizeMode(mode: Int): Int {
        return when (mode) {
            MODE_NON_RU -> MODE_NON_RU
            MODE_GEMINI -> MODE_GEMINI
            else -> MODE_ANY
        }
    }

    private fun currentProfileIds(): List<Long> {
        return listOf(DataStore.selectedProxy, DataStore.startedProfile)
            .filter { it > 0L }
            .distinct()
    }

    private fun reserveTarget(): Int {
        return DataStore.autoVpnWorkingTarget.coerceAtLeast(1)
    }

    private fun initialSelectionWindowMillis(): Long {
        return DataStore.autoVpnInitialSelectionTimeoutSeconds.coerceAtLeast(1) * 1000L
    }

    private fun quarantineMillis(): Long {
        return DataStore.autoVpnQuarantineMinutes.coerceAtLeast(1) * 60L * 1000L
    }

    private fun storedReserveCount(mode: Int = DataStore.autoVpnMode): Int {
        return when (sanitizeMode(mode)) {
            MODE_NON_RU -> DataStore.autoVpnReserveCountNonRu.coerceAtLeast(0)
            MODE_GEMINI -> DataStore.autoVpnReserveCountGemini.coerceAtLeast(0)
            else -> DataStore.autoVpnReserveCountAny.coerceAtLeast(0)
        }
    }

    private fun reserveCount(mode: Int = DataStore.autoVpnMode): Int {
        return reserveProfileIds(mode).size
    }

    private fun persistReserveCounts() {
        persistReserveCount(MODE_ANY)
        persistReserveCount(MODE_NON_RU)
        persistReserveCount(MODE_GEMINI)
    }

    private fun persistReserveCount(mode: Int) {
        val count = reserveProfileIds(mode).size
        when (sanitizeMode(mode)) {
            MODE_NON_RU -> DataStore.autoVpnReserveCountNonRu = count
            MODE_GEMINI -> DataStore.autoVpnReserveCountGemini = count
            else -> DataStore.autoVpnReserveCountAny = count
        }
    }

    private fun clearReserveCounts() {
        DataStore.autoVpnReserveCountAny = 0
        DataStore.autoVpnReserveCountNonRu = 0
        DataStore.autoVpnReserveCountGemini = 0
        DataStore.autoVpnReserveIdsAny = ""
        DataStore.autoVpnReserveIdsNonRu = ""
        DataStore.autoVpnReserveIdsGemini = ""
        synchronized(workingPools) {
            workingPools.clear()
            hydratedReserveModes.clear()
        }
    }

    private fun quarantineCount(mode: Int = DataStore.autoVpnMode): Int {
        val now = System.currentTimeMillis()
        var count = 0
        failedUntil.keys.forEach { profileId ->
            val until = failedUntil[profileId]
            if (until != null && until <= now) {
                failedUntil.remove(profileId)
            } else if (until != null) {
                count += 1
            }
        }
        return count
    }

    private fun reserveProfileIds(mode: Int = DataStore.autoVpnMode): List<Long> {
        val safeMode = sanitizeMode(mode)
        ensureReservePoolLoaded(safeMode)
        val activeIds = currentProfileIds().toSet()
        val manualIds = manualCycleIds[safeMode].orEmpty()
        val retained = LinkedHashSet<Long>()
        val ids = synchronized(workingPools) {
            workingPools[safeMode].orEmpty().toList()
        }

        for (id in ids) {
            if (id <= 0L || id in activeIds || id in manualIds || isMarkedFailed(id)) {
                workingCache.remove(id)
                continue
            }
            val profile = SagerDatabase.proxyDao.getById(id)
            if (profile == null || !profileAllowed(profile, safeMode)) {
                workingCache.remove(id)
                continue
            }
            val cached = workingCache[id]
            if (cached != null && System.currentTimeMillis() - cached.at > RECENT_WORKING_TTL_MS) {
                workingCache.remove(id)
                continue
            }
            retained += id
        }

        synchronized(workingPools) {
            workingPools[safeMode] = retained.toList()
        }
        persistReserveIds(safeMode, retained)
        return retained.toList()
    }

    private fun ensureReservePoolLoaded(mode: Int) {
        val safeMode = sanitizeMode(mode)
        synchronized(workingPools) {
            if (!hydratedReserveModes.add(safeMode)) return
            workingPools[safeMode] = storedReserveIds(safeMode)
        }
    }

    private fun storedReserveIds(mode: Int): List<Long> {
        val encoded = when (sanitizeMode(mode)) {
            MODE_NON_RU -> DataStore.autoVpnReserveIdsNonRu
            MODE_GEMINI -> DataStore.autoVpnReserveIdsGemini
            else -> DataStore.autoVpnReserveIdsAny
        }
        return encoded
            .split(',')
            .mapNotNull { it.toLongOrNull() }
            .filter { it > 0L }
            .distinct()
    }

    private fun persistReserveIds(mode: Int, profileIds: Collection<Long>) {
        val encoded = profileIds
            .asSequence()
            .filter { it > 0L }
            .distinct()
            .joinToString(",")
        when (sanitizeMode(mode)) {
            MODE_NON_RU -> DataStore.autoVpnReserveIdsNonRu = encoded
            MODE_GEMINI -> DataStore.autoVpnReserveIdsGemini = encoded
            else -> DataStore.autoVpnReserveIdsAny = encoded
        }
    }

    fun syncVisibleStateFromStore() {
        clearStaleStoredProgressIfNeeded()
        notificationStatus = DataStore.autoVpnStatusText.takeIf { it.isNotBlank() }
        notificationDetailStatus = DataStore.autoVpnDetailStatusText.takeIf { it.isNotBlank() }
        notificationBriefStatus = DataStore.autoVpnNotificationText.takeIf { it.isNotBlank() }
        val snapshot = storedAutoProgressSnapshot()
        synchronized(progressLock) {
            if (snapshot != null && snapshot.generation >= autoProgressGeneration) {
                autoProgressGeneration = snapshot.generation
                autoRunning = snapshot.running
                autoTotal = snapshot.total
                autoFinished = snapshot.finished
                autoDisplayedFinished = snapshot.finished
                autoWorking = snapshot.working
            } else if (snapshot == null) {
                autoProgressGeneration = DataStore.autoVpnProgressGeneration
                autoRunning = DataStore.autoVpnProgressRunning
                autoTotal = DataStore.autoVpnProgressTotal
                autoFinished = DataStore.autoVpnProgressFinished
                autoDisplayedFinished = DataStore.autoVpnProgressFinished
                autoWorking = DataStore.autoVpnProgressWorking
            }
        }
        lastCheckedPingMillis = DataStore.autoVpnLastPingMillis.takeIf { it > 0 }
        lastCheckedPingFailed = DataStore.autoVpnLastPingFailed
        testResultsResetGeneration = DataStore.autoVpnResultsResetGeneration
    }

    private fun clearStaleStoredProgressIfNeeded() {
        if (!(storedAutoProgressSnapshot()?.running ?: DataStore.autoVpnProgressRunning)) return
        if (SagerNet.started || DataStore.startedProfile > 0L) return
        val updatedAt = DataStore.autoVpnStatusUpdatedAt
        if (updatedAt <= 0L) return
        val staleAfter = maxOf(120_000L, DataStore.connectionTestProgressIntervalSeconds.coerceAtLeast(1) * 4_000L)
        if (System.currentTimeMillis() - updatedAt < staleAfter) return

        clearStoredProgressState()
    }

    private fun clearStoredProgressState() {
        synchronized(progressLock) {
            beginAutoProgressGenerationLocked()
            autoRunning = false
            autoTotal = 0
            autoFinished = 0
            autoDisplayedFinished = 0
            autoWorking = 0
            lastAutoProgressNotifyAt = 0L
        }
        persistAutoProgress()
        DataStore.autoVpnNotificationText = ""
        cancelAutoSearchNotification()
    }

    private fun touchAutoState() {
        DataStore.autoVpnStatusUpdatedAt = System.currentTimeMillis()
    }

    private fun persistAutoStatus() {
        DataStore.autoVpnStatusText = notificationStatus.orEmpty()
        DataStore.autoVpnDetailStatusText = notificationDetailStatus.orEmpty()
        DataStore.autoVpnNotificationText = notificationBriefStatus.orEmpty()
        touchAutoState()
    }

    private fun persistAutoProgress(forceReset: Boolean = false) {
        val snapshot = synchronized(progressLock) {
            AutoProgressSnapshot(
                generation = autoProgressGeneration,
                running = autoRunning,
                total = autoTotal,
                finished = autoFinished,
                working = autoWorking,
            )
        }
        if (snapshot.generation < DataStore.autoVpnProgressGeneration) {
            DiagnosticLog.warn(
                "auto-vpn",
                "progress-write-skipped-stale local=${snapshot.generation} " +
                        "stored=${DataStore.autoVpnProgressGeneration}"
            )
            return
        }
        DataStore.autoVpnProgressGeneration = snapshot.generation
        DataStore.autoVpnProgressSnapshot = snapshot.encode()
        DataStore.autoVpnProgressRunning = snapshot.running
        DataStore.autoVpnProgressTotal = snapshot.total
        DataStore.autoVpnProgressFinished = snapshot.finished
        DataStore.autoVpnProgressWorking = snapshot.working
        synchronized(progressLock) {
            if (autoProgressGeneration == snapshot.generation) {
                autoDisplayedFinished = snapshot.finished
            }
        }
        touchAutoState()
        updateAutoSearchNotification()
        if (forceReset) {
            DiagnosticLog.event(
                "auto-vpn",
                "progress-force-reset generation=${snapshot.generation} total=${snapshot.total} finished=${snapshot.finished}",
            )
        }
    }

    private fun storedAutoProgressSnapshot(): AutoProgressSnapshot? {
        val fields = DataStore.autoVpnProgressSnapshot.split('|')
        if (fields.size != 5) return null
        val generation = fields[0].toLongOrNull() ?: return null
        val running = when (fields[1]) {
            "1" -> true
            "0" -> false
            else -> return null
        }
        val total = fields[2].toIntOrNull() ?: return null
        val finished = fields[3].toIntOrNull() ?: return null
        val working = fields[4].toIntOrNull() ?: return null
        if (generation < 0L || total < 0 || finished < 0 || working < 0) return null
        return AutoProgressSnapshot(generation, running, total, finished, working)
    }

    private fun persistAutoPing() {
        DataStore.autoVpnLastPingMillis = lastCheckedPingMillis ?: 0
        DataStore.autoVpnLastPingFailed = lastCheckedPingFailed
        touchAutoState()
    }

    private fun recordLastExitCountry(profileId: Long, country: String?) {
        if (profileId <= 0L) return
        DataStore.autoVpnLastExitCountry = country.orEmpty()
        DataStore.autoVpnLastExitCountryProfile = profileId
        DataStore.autoVpnLastExitCountryCheckedAt = System.currentTimeMillis()
    }

    private fun lastExitCountryText(): String? {
        val activeProfile = DataStore.startedProfile
        if (activeProfile <= 0L || DataStore.autoVpnLastExitCountryProfile != activeProfile) return null
        val country = DataStore.autoVpnLastExitCountry.ifBlank {
            app.getString(R.string.auto_vpn_unknown_country)
        }
        return app.getString(R.string.auto_vpn_last_exit_country, country)
    }

    private fun candidateCountText(mode: Int = DataStore.autoVpnMode): String? {
        if (!DataStore.autoVpnEnabled) return null
        return reserveStatus(mode)
    }

    private fun updateAutoReserve(mode: Int = DataStore.autoVpnMode) {
        persistReserveCount(mode)
        persistAutoProgress()
        notifyAutoStateChanged()
    }

    private fun consumeReserve(profileId: Long, mode: Int = DataStore.autoVpnMode) {
        if (profileId <= 0L) return
        val safeMode = sanitizeMode(mode)
        ensureReservePoolLoaded(safeMode)
        synchronized(workingPools) {
            workingPools[safeMode] = workingPools[safeMode].orEmpty().filterNot { it == profileId }
        }
        updateAutoReserve(safeMode)
    }

    private fun markScanned(mode: Int, profileId: Long): Boolean {
        if (profileId <= 0L) return false
        val safeMode = sanitizeMode(mode)
        return synchronized(scannedIds) {
            scannedIds.getOrPut(safeMode) { LinkedHashSet() }.add(profileId)
        }
    }

    private suspend fun restartAutoScanCycle(mode: Int, scannedTotal: Int, testedAnyInCycle: Boolean): Boolean {
        val safeMode = sanitizeMode(mode)
        val completedProgressCycle = synchronized(progressLock) {
            autoTotal == scannedTotal && autoFinished >= autoTotal
        }
        if (scannedTotal <= 0 || (!testedAnyInCycle && !completedProgressCycle)) return false
        DiagnosticLog.critical(
            "auto-vpn",
            "restart-scan-cycle mode=$safeMode scanned=$scannedTotal testedAny=$testedAnyInCycle " +
                    "completedProgress=$completedProgressCycle " +
                    "progress=$autoFinished/$autoTotal working=$autoWorking"
        )

        AutoProfileScoreStore.flush()
        val subscriptions = SagerDatabase.groupDao.subscriptions()
        if (subscriptions.isNotEmpty()) {
            setAutoStatus(
                safeMode,
                R.string.auto_vpn_status_updating_subscriptions,
                0,
                subscriptions.size,
            )
            try {
                val summary = SubscriptionUpdater.updateGroups(
                    subscriptions,
                    byUser = false,
                    forceUpdateWhenDisconnected = true,
                    onProgress = { progress ->
                        setAutoStatus(
                            safeMode,
                            R.string.auto_vpn_status_updating_subscriptions,
                            progress.done,
                            progress.total,
                        )
                    },
                    waitForExistingUpdate = true,
                )
                if (DataStore.subscriptionAutoSortGroups) {
                    GroupManager.sortGroupsByTotalProfiles()
                }
                DiagnosticLog.critical(
                    "auto-vpn",
                    "cycle-subscription-update-finished mode=$safeMode total=${summary.total} " +
                            "success=${summary.success} errors=${summary.errors} skipped=${summary.skipped}",
                )
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                DiagnosticLog.error("auto-vpn", "cycle-subscription-update-failed mode=$safeMode", e)
                Logs.w(e)
            }
        } else {
            DiagnosticLog.event("auto-vpn", "cycle-subscription-update-skipped mode=$safeMode reason=no-subscriptions")
        }

        if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode) return false

        val nextProfiles = autoScanProfiles(enabledGroupIds())
        val nextTotal = nextProfiles.size
        val heldProfileIds = (currentProfileIds() + reserveProfileIds(safeMode))
            .filterNot { isMarkedFailed(it) }
            .toSet()
        val remainingProfiles = nextProfiles
            .filter { profileAllowed(it, safeMode) && it.id !in heldProfileIds }
        val allRemainingQuarantined = remainingProfiles.isNotEmpty() &&
                remainingProfiles.all { isMarkedFailed(it.id) }
        if (allRemainingQuarantined) {
            failedUntil.clear()
            DiagnosticLog.critical(
                "auto-vpn",
                "quarantine-reset-all-eligible-profiles mode=$safeMode profiles=${remainingProfiles.size}",
            )
        }
        synchronized(scannedIds) {
            scannedIds.remove(safeMode)
        }
        persistReserveCount(safeMode)
        resetAutoProgressForNextCycle(safeMode, nextTotal)
        setAutoStatus(safeMode, R.string.auto_vpn_status_restart_scan)
        delay(AUTO_SCAN_NEXT_CYCLE_DELAY_MS)
        return true
    }

    private fun stopScan(feedback: Boolean) {
        DiagnosticLog.critical(
            "auto-vpn",
            "stop-scan feedback=$feedback activeScan=${autoScanJob?.isActive == true} " +
                    "progress=$autoFinished/$autoTotal working=$autoWorking"
        )
        autoScanJob?.cancel()
        autoScanJob = null
        finishAutoProgress()
        setAutoStatus(DataStore.autoVpnMode, R.string.auto_vpn_status_scan_stopped)
        if (feedback) {
            notifyUser(R.string.auto_vpn_scan_stopped)
        }
    }

    private fun resetAutoSearchState(cancelScan: Boolean) {
        if (cancelScan) {
            autoScanJob?.cancel()
            autoScanJob = null
        }
        workingCache.clear()
        synchronized(workingPools) {
            workingPools.clear()
        }
        manualCycleIds.clear()
        synchronized(scannedIds) {
            scannedIds.clear()
        }
        lastCheckedPingMillis = null
        lastCheckedPingFailed = false
        persistAutoPing()
        failures = 0
        clearAutoProgress()
    }

    private fun resetAutoCandidateResults(groupIds: List<Long>, mode: Int) {
        ProfileTestRunner.syncFromStore()
        if (ProfileTestRunner.isRunning()) {
            DiagnosticLog.event(
                "auto-vpn",
                "candidate-results-reset-skipped mode=${sanitizeMode(mode)} reason=manual-test-active " +
                        "vip-score-store=retained",
            )
            return
        }
        groupIds.chunked(500).forEach {
            SagerDatabase.proxyDao.clearTestResultsByGroups(it)
        }
        markAutoTestResultsReset()
        DiagnosticLog.critical(
            "auto-vpn",
            "candidate-results-cleared mode=${sanitizeMode(mode)} groups=${groupIds.size} " +
                    "vip-score-store=retained",
        )
    }

    private fun automaticExcludedIds(mode: Int, extraIds: Collection<Long>): Set<Long> {
        return (manualCycleIds[mode].orEmpty() + extraIds)
            .filter { it > 0L }
            .toSet()
    }

    private fun resetAutoProgress(total: Int, resetFinished: Boolean = true) {
        synchronized(progressLock) {
            beginAutoProgressGenerationLocked()
            autoRunning = true
            autoTotal = total.coerceAtLeast(0)
            autoFinished = if (resetFinished) {
                0
            } else if (autoTotal > 0) {
                autoFinished.coerceIn(0, autoTotal)
            } else {
                autoFinished.coerceAtLeast(0)
            }
            if (resetFinished) autoDisplayedFinished = 0
            autoWorking = if (resetFinished) 0 else autoWorking
            lastAutoProgressNotifyAt = System.currentTimeMillis()
        }
        DiagnosticLog.event(
            "auto-vpn",
            "progress-reset total=$autoTotal resetFinished=$resetFinished finished=$autoFinished working=$autoWorking"
        )
        persistAutoProgress()
        notifyAutoStateChanged()
    }

    private fun beginOnDemandSearchProgress(mode: Int, total: Int): Boolean {
        // A direct search may continue a scan started in another process. Read the
        // persisted snapshot before claiming the progress panel, otherwise a switch
        // briefly replaces an already-running counter with 0/total.
        syncVisibleStateFromStore()
        if (autoScanJob?.isActive == true) return false
        val safeMode = sanitizeMode(mode)
        val retainedWorking = reserveCount(safeMode)
        val stored = storedAutoProgressSnapshot()
        val completedStoredCycle = stored != null &&
                !stored.running &&
                stored.total == total.coerceAtLeast(0) &&
                total > 0 &&
                stored.finished >= total
        if (completedStoredCycle) {
            synchronized(scannedIds) {
                scannedIds.remove(safeMode)
            }
            DiagnosticLog.critical(
                "auto-vpn",
                "progress-on-demand-new-cycle mode=$safeMode total=$total finished=${stored!!.finished}"
            )
            resetAutoProgressForNextCycle(safeMode, total)
            return true
        }
        if (stored?.running == true && stored.total == total.coerceAtLeast(0)) {
            synchronized(progressLock) {
                autoProgressGeneration = maxOf(autoProgressGeneration, stored.generation)
                autoRunning = true
                autoTotal = stored.total
                autoFinished = stored.finished.coerceIn(0, stored.total)
                autoDisplayedFinished = autoFinished
                autoWorking = maxOf(stored.working, retainedWorking)
                lastAutoProgressNotifyAt = 0L
            }
            DiagnosticLog.event(
                "auto-vpn",
                "progress-on-demand-resume mode=$safeMode total=$autoTotal " +
                        "finished=$autoFinished working=$autoWorking"
            )
            notifyAutoStateChanged()
            return true
        }
        synchronized(progressLock) {
            beginAutoProgressGenerationLocked()
            autoRunning = true
            autoTotal = total.coerceAtLeast(0)
            autoFinished = 0
            autoDisplayedFinished = 0
            autoWorking = retainedWorking
            lastAutoProgressNotifyAt = System.currentTimeMillis()
        }

        DiagnosticLog.event(
            "auto-vpn",
            "progress-on-demand mode=$safeMode total=$autoTotal retainedWorking=$retainedWorking"
        )
        persistAutoProgress()
        notifyAutoStateChanged()
        return true
    }

    private fun resetAutoProgressForNextCycle(mode: Int, total: Int) {
        val safeMode = sanitizeMode(mode)
        val retainedWorking = maxOf(autoWorking, reserveCount(safeMode))
        synchronized(progressLock) {
            beginAutoProgressGenerationLocked()
            autoRunning = true
            autoTotal = total.coerceAtLeast(0)
            autoFinished = 0
            autoDisplayedFinished = 0
            autoWorking = retainedWorking.coerceAtLeast(0)
            lastAutoProgressNotifyAt = 0L
        }
        DiagnosticLog.event(
            "auto-vpn",
            "progress-next-cycle mode=$safeMode total=$autoTotal finished=0 retainedWorking=$retainedWorking"
        )
        persistAutoProgress(forceReset = true)
        notifyAutoStateChanged()
    }

    private fun clearAutoProgress() {
        synchronized(progressLock) {
            beginAutoProgressGenerationLocked()
            autoRunning = false
            autoTotal = 0
            autoFinished = 0
            autoDisplayedFinished = 0
            autoWorking = 0
            lastAutoProgressNotifyAt = 0L
        }
        DiagnosticLog.event("auto-vpn", "progress-clear")
        clearReserveCounts()
        persistAutoProgress()
        notifyAutoStateChanged()
    }

    private fun beginAutoProgressGenerationLocked() {
        autoProgressGeneration = maxOf(
            System.currentTimeMillis(),
            autoProgressGeneration + 1L,
            DataStore.autoVpnProgressGeneration + 1L,
        )
    }

    private fun markAutoTestResultsReset() {
        testResultsResetGeneration += 1
        DataStore.autoVpnResultsResetGeneration = testResultsResetGeneration
        notifyAutoStateChanged()
    }

    private fun recordLastPing(ping: Int?) {
        lastCheckedPingMillis = ping
        lastCheckedPingFailed = ping == null
        persistAutoPing()
    }

    private fun lastPingText(): String? {
        lastCheckedPingMillis?.let {
            return app.getString(R.string.auto_vpn_last_ping, it)
        }
        return if (lastCheckedPingFailed) {
            app.getString(R.string.auto_vpn_last_ping_error)
        } else {
            null
        }
    }

    private fun hasDetailedAppStatus(): Boolean {
        return notificationDetailStatus != null ||
                notificationStatus != null ||
                autoRunning ||
                autoTotal > 0 ||
                lastCheckedPingMillis != null ||
                lastCheckedPingFailed
    }

    private fun appStatusText(mode: Int = DataStore.autoVpnMode): String {
        val parts = ArrayList<String>()
        parts += notificationDetailStatus
            ?: notificationStatus
            ?: app.getString(R.string.auto_vpn_notification_running, modeName(mode))
        lastDecisionReason?.let { parts += it }
        if (autoTotal > 0) {
            parts += app.getString(R.string.auto_vpn_checked_count, autoDisplayedFinished, autoTotal)
            parts += reserveStatus(mode)
        }
        lastPingText()?.let { parts += it }
        return parts.joinToString(" | ")
    }

    private fun finishAutoProgress() {
        synchronized(progressLock) {
            autoRunning = false
            lastAutoProgressNotifyAt = 0L
        }
        DiagnosticLog.event("auto-vpn", "progress-finish finished=$autoFinished total=$autoTotal working=$autoWorking")
        persistAutoProgress()
        AutoProfileScoreStore.flush()
        notifyAutoStateChanged()
    }

    private fun recordAutoTestResult(working: Boolean) {
        val shouldNotify: Boolean
        synchronized(progressLock) {
            autoFinished = if (autoTotal > 0) {
                (autoFinished + 1).coerceAtMost(autoTotal)
            } else {
                autoFinished + 1
            }
            if (working) {
                autoWorking += 1
            }
            val now = System.currentTimeMillis()
            shouldNotify = now - lastAutoProgressNotifyAt >= progressUpdateIntervalMillis() ||
                    (autoTotal > 0 && autoFinished >= autoTotal)
            if (shouldNotify) {
                lastAutoProgressNotifyAt = now
            }
        }
        if (shouldNotify) {
            DiagnosticLog.event(
                "auto-vpn",
                "progress-count finished=$autoFinished total=$autoTotal working=$autoWorking workingResult=$working"
            )
            persistAutoProgress()
            notifyAutoStateChanged()
        }
    }

    private fun recordAutoSkippedResults(count: Int) {
        if (count <= 0) return
        val shouldNotify: Boolean
        synchronized(progressLock) {
            autoFinished = if (autoTotal > 0) {
                (autoFinished + count).coerceAtMost(autoTotal)
            } else {
                autoFinished + count
            }
            val now = System.currentTimeMillis()
            shouldNotify = now - lastAutoProgressNotifyAt >= progressUpdateIntervalMillis() ||
                    (autoTotal > 0 && autoFinished >= autoTotal)
            if (shouldNotify) {
                lastAutoProgressNotifyAt = now
            }
        }
        if (shouldNotify) {
            DiagnosticLog.event(
                "auto-vpn",
                "progress-skip count=$count finished=$autoFinished total=$autoTotal working=$autoWorking"
            )
            persistAutoProgress()
            notifyAutoStateChanged()
        }
    }

    /**
     * Older builds used a second notification for scan progress. Direct checks now
     * use the VPN notification ID, so make sure a stale legacy notification is gone.
     */
    private fun updateAutoSearchNotification() {
        cancelAutoSearchNotification()
    }

    private fun cancelAutoSearchNotification() {
        runCatching {
            NotificationManagerCompat.from(app).cancel(AUTO_SEARCH_NOTIFICATION_ID)
        }.onFailure {
            Logs.w(it)
        }
    }

    private fun notifyAutoStateChanged() {
        app.sendBroadcast(Intent(Action.AUTO_STATUS_CHANGED).setPackage(app.packageName))
        val snapshot = synchronized(listeners) {
            listeners.toList()
        }
        if (snapshot.isEmpty()) return
        runOnMainDispatcher {
            snapshot.forEach { it.onAutoVpnStateChanged() }
        }
    }

    private fun setAutoStatus(mode: Int, resId: Int, vararg args: Any) {
        val messageArgs = arrayOf<Any>(modeName(mode), *args)
        setAutoStatus(
            app.getString(resId, *messageArgs),
            briefStatus = notificationBriefForStatus(resId),
        )
    }

    private fun setAutoAppStatus(mode: Int, resId: Int, vararg args: Any) {
        val messageArgs = arrayOf<Any>(modeName(mode), *args)
        setAutoStatus(
            app.getString(resId, *messageArgs),
            clearNotificationStatus = false,
            briefStatus = notificationBriefForStatus(resId),
        )
    }

    private fun setAutoNotificationStatus(mode: Int, resId: Int, vararg args: Any) {
        val messageArgs = arrayOf<Any>(modeName(mode), *args)
        val message = app.getString(resId, *messageArgs)
        setAutoNotificationStatus(message, notificationBriefForStatus(resId))
    }

    private fun setAutoNotificationStatus(message: String, briefStatus: String?) {
        notificationDetailStatus = message
        notificationStatus = message
        notificationBriefStatus = briefStatus
        DiagnosticLog.event("auto-vpn", "notification-status=$message brief=${briefStatus.orEmpty()}")
        persistAutoStatus()
        notifyAutoStateChanged()
    }

    private fun setActiveFailureStatus(mode: Int, ping: Int?, attempt: Int, maxAttempts: Int) {
        if (ping != null) {
            val message = app.getString(
                R.string.auto_vpn_status_active_bad_ping_count,
                modeName(mode),
                ping,
                badPingMillis(),
                attempt,
                maxAttempts,
            )
            val brief = app.getString(
                R.string.auto_vpn_notification_bad_ping_attempt,
                attempt,
                maxAttempts,
            )
            setAutoNotificationStatus(message, brief)
        } else {
            val message = app.getString(
                R.string.auto_vpn_status_active_issue,
                modeName(mode),
                attempt,
                maxAttempts,
            )
            val brief = app.getString(
                R.string.auto_vpn_notification_failed_attempt,
                attempt,
                maxAttempts,
            )
            setAutoNotificationStatus(message, brief)
        }
    }

    private fun setAutoReadyStatus(
        mode: Int = DataStore.autoVpnMode,
        scheduleServices: Boolean = true,
    ) {
        val safeMode = sanitizeMode(mode)
        if (!DataStore.autoVpnEnabled || DataStore.autoVpnMode != safeMode) return
        val current = SagerDatabase.proxyDao.getById(DataStore.selectedProxy)
        if ((SagerNet.started || DataStore.startedProfile > 0L || DataStore.autoVpnServiceStartRequested) && current != null) {
            setAutoStatus(safeMode, R.string.auto_vpn_status_ready, current.displayName())
            if (scheduleServices) scheduleServiceAvailabilityCheck(DataStore.startedProfile, safeMode)
        } else {
            setAutoStatus(null)
        }
    }

    private fun refreshServiceAvailabilityAfterStableCheck(profileId: Long, mode: Int) {
        if (!isSameStartedAutoProfile(profileId, mode)) {
            DiagnosticLog.event(
                "auto-vpn-services",
                "post-connect-availability-not-scheduled profile=$profileId mode=$mode reason=profile-not-current",
            )
            return
        }
        synchronized(serviceAvailabilityLock) {
            serviceAvailabilityAfterConnectJob?.cancel()
            serviceAvailabilityAfterConnectJob = runOnDefaultDispatcher {
                try {
                    delay(2_000L)
                    repeat(5) { attempt ->
                        if (!isSameSelectedAutoProfile(profileId, mode)) {
                            DiagnosticLog.event(
                                "auto-vpn-services",
                                "post-connect-availability-skipped profile=$profileId mode=$mode reason=profile-changed",
                            )
                            return@runOnDefaultDispatcher
                        }
                        if (SagerNet.started) {
                            DiagnosticLog.event(
                                "auto-vpn-services",
                                "post-connect-availability-start profile=$profileId mode=$mode " +
                                        "tunnelReadyAfterAttempts=${attempt + 1}",
                            )
                            scheduleServiceAvailabilityCheck(profileId, mode, force = true)
                            return@runOnDefaultDispatcher
                        }
                        delay(1_000L)
                    }
                    DiagnosticLog.warn(
                        "auto-vpn-services",
                        "post-connect-availability-failed profile=$profileId mode=$mode reason=tunnel-not-ready",
                    )
                } finally {
                    val currentJob = currentCoroutineContext()[Job]
                    synchronized(serviceAvailabilityLock) {
                        if (serviceAvailabilityAfterConnectJob === currentJob) {
                            serviceAvailabilityAfterConnectJob = null
                        }
                    }
                }
            }
        }
    }

    private fun scheduleServiceAvailabilityFallback(profileId: Long, mode: Int) {
        val safeMode = sanitizeMode(mode)
        if (profileId <= 0L) return
        synchronized(serviceAvailabilityLock) {
            if (serviceAvailabilityAfterConnectJob?.isActive == true) return
            DiagnosticLog.event(
                "auto-vpn-services",
                "post-connect-availability-fallback-scheduled profile=$profileId mode=$safeMode",
            )
            serviceAvailabilityAfterConnectJob = runOnDefaultDispatcher {
                try {
                    delay(POST_CONNECT_VERIFY_DELAYS_MS.sum() + 2_000L)
                    if (!isSameSelectedAutoProfile(profileId, safeMode)) {
                        DiagnosticLog.event(
                            "auto-vpn-services",
                            "post-connect-availability-fallback-skipped profile=$profileId mode=$safeMode reason=profile-changed",
                        )
                        return@runOnDefaultDispatcher
                    }
                    if (hasServiceAvailabilityState(profileId, safeMode)) {
                        DiagnosticLog.event(
                            "auto-vpn-services",
                            "post-connect-availability-fallback-skipped profile=$profileId mode=$safeMode reason=check-already-started",
                        )
                        return@runOnDefaultDispatcher
                    }

                    repeat(5) { attempt ->
                        if (!isSameSelectedAutoProfile(profileId, safeMode)) {
                            return@runOnDefaultDispatcher
                        }
                        if (SagerNet.started) {
                            DiagnosticLog.warn(
                                "auto-vpn-services",
                                "post-connect-availability-fallback-start profile=$profileId mode=$safeMode " +
                                        "tunnelReadyAfterAttempts=${attempt + 1}",
                            )
                            scheduleServiceAvailabilityCheck(profileId, safeMode)
                            return@runOnDefaultDispatcher
                        }
                        delay(1_000L)
                    }
                    DiagnosticLog.warn(
                        "auto-vpn-services",
                        "post-connect-availability-fallback-failed profile=$profileId mode=$safeMode reason=tunnel-not-ready",
                    )
                } finally {
                    val currentJob = currentCoroutineContext()[Job]
                    synchronized(serviceAvailabilityLock) {
                        if (serviceAvailabilityAfterConnectJob === currentJob) {
                            serviceAvailabilityAfterConnectJob = null
                        }
                    }
                }
            }
        }
    }

    private fun isSameSelectedAutoProfile(profileId: Long, mode: Int): Boolean =
        DataStore.autoVpnEnabled &&
                DataStore.autoVpnMode == sanitizeMode(mode) &&
                DataStore.startedProfile == profileId

    private fun hasServiceAvailabilityState(profileId: Long, mode: Int): Boolean = synchronized(serviceAvailabilityLock) {
        val sameTarget = serviceAvailabilityProfileId == profileId &&
                serviceAvailabilityMode == sanitizeMode(mode)
        sameTarget && (
                serviceAvailabilityResults.any {
                    it.state != ServiceAvailabilityProbe.State.CHECKING &&
                            it.state != ServiceAvailabilityProbe.State.NOT_CHECKED
                } ||
                        (serviceAvailabilityJob?.isActive == true &&
                                System.currentTimeMillis() - serviceAvailabilityCheckedAt < SERVICE_AVAILABILITY_STALE_CHECK_MS)
                )
    }

    private fun setAutoStatus(
        status: String?,
        clearNotificationStatus: Boolean = true,
        briefStatus: String? = null,
    ) {
        notificationStatus = status
        if (clearNotificationStatus) {
            notificationDetailStatus = null
        }
        notificationBriefStatus = briefStatus
        DiagnosticLog.event(
            "auto-vpn",
            "status=${status.orEmpty()} detail=${notificationDetailStatus.orEmpty()} brief=${briefStatus.orEmpty()} " +
                    "clearDetail=$clearNotificationStatus"
        )
        persistAutoStatus()
        notifyAutoStateChanged()
    }

    private fun notificationBriefForStatus(resId: Int): String? {
        return when (resId) {
            R.string.auto_vpn_status_initial_search,
            R.string.auto_vpn_status_initial_found,
            R.string.auto_vpn_status_testing_more,
            R.string.auto_vpn_status_restart_scan,
            R.string.auto_vpn_status_rechecking_reserve -> app.getString(R.string.auto_vpn_notification_searching)
            R.string.auto_vpn_status_selecting,
            R.string.auto_vpn_status_switched_to -> app.getString(R.string.auto_vpn_notification_switching)
            R.string.auto_vpn_status_prechecking_candidate,
            R.string.auto_vpn_status_verifying_connected,
            R.string.auto_vpn_status_checking_active -> app.getString(R.string.auto_vpn_notification_checking)
            R.string.auto_vpn_status_no_candidate -> null
            R.string.auto_vpn_status_scan_stopped -> app.getString(R.string.auto_vpn_notification_scan_stopped)
            else -> null
        }
    }

    private fun notifyUser(resId: Int, vararg args: Any) {
        runOnDefaultDispatcher {
            onMainDispatcher {
                Toast.makeText(app, app.getString(resId, *args), Toast.LENGTH_SHORT).show()
            }
        }
    }

    private fun checkIntervalMillis(): Long {
        return DataStore.autoVpnCheckIntervalSeconds.coerceAtLeast(10) * 1000L
    }

    private fun autoProgressNotificationText(): String {
        val total = autoTotal.coerceAtLeast(0)
        val finished = if (total > 0) {
            autoDisplayedFinished.coerceIn(0, total)
        } else {
            autoDisplayedFinished.coerceAtLeast(0)
        }
        return app.getString(R.string.connection_test_progress, finished, total)
    }

    private fun progressUpdateIntervalMillis(): Long {
        return DataStore.connectionTestProgressIntervalSeconds.coerceAtLeast(1) * 1000L
    }

    private fun badPingMillis(): Int {
        return DataStore.autoVpnBadPingMillis.coerceAtLeast(1)
    }

    private fun failuresBeforeSwitch(): Int {
        return DataStore.autoVpnFailuresBeforeSwitch.coerceAtLeast(1)
    }

    private fun switchCooldownMillis(): Long {
        return DataStore.autoVpnSwitchCooldownSeconds.coerceAtLeast(0) * 1000L
    }
}
