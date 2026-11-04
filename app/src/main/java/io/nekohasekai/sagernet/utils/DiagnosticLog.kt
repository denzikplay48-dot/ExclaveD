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

package io.nekohasekai.sagernet.utils

import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.Process
import android.util.Log
import io.nekohasekai.sagernet.BuildConfig
import io.nekohasekai.sagernet.LogLevel
import io.nekohasekai.sagernet.SagerNet
import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.ktx.app
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

object DiagnosticLog {

    private const val TAG = "DiagnosticLog"
    private const val MAX_BYTES = 8L * 1024L * 1024L
    private const val KEEP_BYTES = 6L * 1024L * 1024L
    private const val MAX_AGE_MS = 60L * 60L * 1000L
    private const val RETENTION_TRIM_INTERVAL_MS = 60_000L
    private const val TIMESTAMP_PREFIX_LENGTH = 29
    private const val CRASH_REPORT_BYTES = 2 * 1024 * 1024
    private const val WATCHDOG_INTERVAL_MS = 5_000L
    private const val WATCHDOG_STALL_MS = 10_000L

    private val executor: ExecutorService = Executors.newSingleThreadExecutor { task ->
        Thread(task, "diagnostic-log").apply { isDaemon = true }
    }
    private val initialized = AtomicBoolean(false)
    private val watchdogStarted = AtomicBoolean(false)
    private val lock = Any()
    private val timeFormat = ThreadLocal<SimpleDateFormat>()
    private val fileTimeFormat = ThreadLocal<SimpleDateFormat>()

    @Volatile
    private var lastRetentionTrimAtMs = 0L

    @Volatile
    private var processName: String = BuildConfig.APPLICATION_ID

    private val logDirectory: File
        get() = File(app.filesDir, "diagnostic").also { it.mkdirs() }

    val logFile: File
        get() = File(logDirectory, "diagnostic.log")

    private val lockFile: File
        get() = File(logDirectory, "diagnostic.lock")

    fun init(process: String, mainProcess: Boolean) {
        processName = process.ifBlank { BuildConfig.APPLICATION_ID }
        if (mainProcess) endAutoSearchSessionAndClear()
        initialized.set(true)
        critical(
            "app",
            "process-start version=${BuildConfig.VERSION_NAME}(${BuildConfig.VERSION_CODE}) " +
                    "flavor=${BuildConfig.FLAVOR} process=$processName main=$mainProcess " +
                    "pid=${Process.myPid()} sdk=${Build.VERSION.SDK_INT} " +
                    "device=${oneLine("${Build.MANUFACTURER} ${Build.MODEL}")}"
        )
        event("app", "initial-state ${stateSummary()}")
    }

    fun beginAutoSearchSession(source: String) {
        if (DataStore.logLevel == LogLevel.NONE) {
            endAutoSearchSessionAndClear()
            return
        }
        synchronized(lock) {
            val sessionId = nextSessionId()
            DataStore.diagnosticJournalSessionId = -sessionId
            clearJournalLocked()
            DataStore.diagnosticJournalSessionId = sessionId
        }
        startMainThreadWatchdog()
        critical("diagnostic", "auto-search-session-start source=${oneLine(source, 80)} ${stateSummary()}")
    }

    fun endAutoSearchSessionAndClear() {
        synchronized(lock) {
            DataStore.diagnosticJournalSessionId = -nextSessionId()
            clearJournalLocked()
        }
    }

    fun onLogLevelChanged(level: Int) {
        if (level == LogLevel.NONE) endAutoSearchSessionAndClear()
    }

    private fun nextSessionId(): Long {
        val current = DataStore.diagnosticJournalSessionId
        val magnitude = if (current == Long.MIN_VALUE) Long.MAX_VALUE else kotlin.math.abs(current)
        val now = System.currentTimeMillis()
        return if (magnitude >= now && magnitude < Long.MAX_VALUE) magnitude + 1L else now
    }

    fun event(area: String, message: String) {
        write("I", area, message, null, sync = false)
    }

    fun critical(area: String, message: String) {
        write("I", area, message, null, sync = true)
    }

    fun warn(area: String, message: String, throwable: Throwable? = null) {
        write("W", area, message, throwable, sync = false)
    }

    fun error(area: String, message: String, throwable: Throwable? = null) {
        write("E", area, message, throwable, sync = true)
    }

    fun crash(thread: Thread, throwable: Throwable) {
        error(
            "crash",
            "uncaught thread=${oneLine(thread.toString())} ${stateSummary()}",
            throwable,
        )
    }

    fun createShareFile(): File {
        critical("diagnostic", "export-requested ${stateSummary()}")
        val directory = File(app.externalCacheDir ?: app.cacheDir, "log").also { it.mkdirs() }
        val target = File(directory, "Exclave Diagnostic Journal ${fileTimestamp()}.log")
        target.writeText(humanReadableHeader(), StandardCharsets.UTF_8)
        FileOutputStream(target, true).use { output ->
            output.write(formatJournalForHuman(readRecentText(MAX_BYTES.toInt())).toByteArray(StandardCharsets.UTF_8))
        }
        return target
    }

    fun readForCrashReport(): String {
        return readRecentText(CRASH_REPORT_BYTES)
    }

    fun stateSummary(): String {
        val runtime = Runtime.getRuntime()
        val usedMemory = runtime.totalMemory() - runtime.freeMemory()
        val pendingCount = runCatching {
            DataStore.profileTestPendingIds.lineSequence().count { it.isNotBlank() }
        }.getOrDefault(-1)
        val updatedAgo = runCatching {
            val updatedAt = DataStore.profileTestUpdatedAt
            if (updatedAt > 0L) System.currentTimeMillis() - updatedAt else -1L
        }.getOrDefault(-1L)
        val profileState = runCatching {
            "profileTest{runId=${DataStore.profileTestRunId},running=${DataStore.profileTestRunning}," +
                    "refreshDeferred=${DataStore.profileTestRefreshDeferred}," +
                    "resume=${DataStore.profileTestResumeAvailable},total=${DataStore.profileTestTotal}," +
                    "finished=${DataStore.profileTestFinished},working=${DataStore.profileTestWorking}," +
                    "pending=$pendingCount,updatedAgoMs=$updatedAgo}"
        }.getOrElse { "profileTest{error=${oneLine(it.toString())}}" }
        val autoState = runCatching {
            "auto{enabled=${DataStore.autoVpnEnabled},mode=${DataStore.autoVpnMode}," +
                    "running=${DataStore.autoVpnProgressRunning}," +
                    "progress=${DataStore.autoVpnProgressFinished}/${DataStore.autoVpnProgressTotal}," +
                    "working=${DataStore.autoVpnProgressWorking},status=${oneLine(DataStore.autoVpnStatusText)}}"
        }.getOrElse { "auto{error=${oneLine(it.toString())}}" }
        val serviceState = runCatching {
            "service{started=${SagerNet.started},selected=${DataStore.selectedProxy}," +
                    "startedProfile=${DataStore.startedProfile},group=${DataStore.selectedGroup}}"
        }.getOrElse { "service{error=${oneLine(it.toString())}}" }
        val memoryState = "memory{used=$usedMemory,total=${runtime.totalMemory()},max=${runtime.maxMemory()}," +
                "threads=${Thread.getAllStackTraces().size}}"
        return "$serviceState $profileState $autoState $memoryState"
    }

    private fun startMainThreadWatchdog() {
        if (!watchdogStarted.compareAndSet(false, true)) return
        Thread({
            val mainHandler = Handler(Looper.getMainLooper())
            var lastStallLoggedAt = 0L
            while (!Thread.currentThread().isInterrupted) {
                try {
                    val sentAt = System.currentTimeMillis()
                    val answered = AtomicBoolean(false)
                    mainHandler.post {
                        answered.set(true)
                    }
                    Thread.sleep(WATCHDOG_INTERVAL_MS)
                    if (!answered.get()) {
                        val stalledFor = System.currentTimeMillis() - sentAt
                        val now = System.currentTimeMillis()
                        if (stalledFor >= WATCHDOG_STALL_MS &&
                            now - lastStallLoggedAt >= WATCHDOG_STALL_MS
                        ) {
                            lastStallLoggedAt = now
                            val mainStack = Looper.getMainLooper().thread.stackTrace.joinToString(" <- ") {
                                "${it.className}.${it.methodName}:${it.lineNumber}"
                            }
                            error(
                                "watchdog",
                                "main-thread-not-responding stalledMs=$stalledFor ${stateSummary()} mainStack=$mainStack",
                            )
                        }
                    }
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                } catch (e: Throwable) {
                    Log.w(TAG, "watchdog failed", e)
                }
            }
        }, "diagnostic-watchdog").apply {
            isDaemon = true
            start()
        }
    }

    private fun write(
        level: String,
        area: String,
        message: String,
        throwable: Throwable?,
        sync: Boolean,
    ) {
        val sessionId = runCatching { DataStore.diagnosticJournalSessionId }.getOrDefault(0L)
        if (!isSessionEnabled(sessionId)) return
        val entry = buildEntry(level, area, message, throwable)
        if (sync || !initialized.get()) {
            writeNow(entry, sessionId)
        } else {
            executor.execute {
                if (isSessionEnabled(sessionId)) writeNow(entry, sessionId)
            }
        }
    }

    private fun isSessionEnabled(sessionId: Long): Boolean = runCatching {
        sessionId > 0L &&
                DataStore.logLevel != LogLevel.NONE &&
                DataStore.diagnosticJournalSessionId == sessionId
    }.getOrDefault(false)

    private fun buildEntry(
        level: String,
        area: String,
        message: String,
        throwable: Throwable?,
    ): String = buildString {
        append(timestamp())
        append(' ')
        append(level)
        append(' ')
        append("pid=")
        append(Process.myPid())
        append(' ')
        append("process=")
        append(oneLine(processName, 160))
        append(' ')
        append("thread=")
        append(oneLine(Thread.currentThread().name, 120))
        append(' ')
        append("area=")
        append(oneLine(area, 80))
        append(" | ")
        append(oneLine(message, 4000))
        append('\n')
        if (throwable != null) {
            append(throwable.stackTraceToString())
            append('\n')
        }
    }

    private fun writeNow(entry: String, sessionId: Long) {
        synchronized(lock) {
            runCatching {
                withFileLock {
                    if (!isSessionEnabled(sessionId)) return@withFileLock
                    val now = System.currentTimeMillis()
                    if (now - lastRetentionTrimAtMs >= RETENTION_TRIM_INTERVAL_MS) {
                        trimOlderEntriesLocked(now)
                        lastRetentionTrimAtMs = now
                    }
                    trimIfNeededLocked()
                    FileOutputStream(logFile, true).use { output ->
                        output.write(entry.toByteArray(StandardCharsets.UTF_8))
                    }
                }
            }.onFailure {
                Log.w(TAG, "write failed", it)
            }
        }
    }

    private fun readRecentText(maxBytes: Int): String {
        return synchronized(lock) {
            runCatching {
                withFileLock {
                    if (!logFile.exists()) return@withFileLock "Persistent diagnostic journal is empty.\n"
                    val now = System.currentTimeMillis()
                    trimOlderEntriesLocked(now)
                    trimIfNeededLocked()
                    lastRetentionTrimAtMs = now
                    val bytes = logFile.readBytes()
                    val safeMax = maxBytes.coerceAtLeast(1)
                    val selected = if (bytes.size > safeMax) {
                        bytes.copyOfRange(bytes.size - safeMax, bytes.size)
                    } else {
                        bytes
                    }
                    val prefix = if (bytes.size > safeMax) {
                        "--- beginning trimmed for export; showing last $safeMax bytes ---\n"
                    } else {
                        ""
                    }
                    prefix + String(selected, StandardCharsets.UTF_8)
                }
            }.getOrElse {
                "Persistent diagnostic journal read failed: ${it.stackTraceToString()}\n"
            }
        }
    }

    private fun <T> withFileLock(block: () -> T): T {
        logDirectory.mkdirs()
        RandomAccessFile(lockFile, "rw").use { lockAccess ->
            lockAccess.channel.use { channel ->
                channel.lock().use {
                    return block()
                }
            }
        }
    }

    private fun clearJournalLocked() {
        runCatching { withFileLock { logFile.delete() } }
            .onFailure { Log.w(TAG, "clear failed", it) }
        lastRetentionTrimAtMs = 0L
    }

    private fun trimIfNeededLocked() {
        if (!logFile.exists() || logFile.length() <= MAX_BYTES) return
        val bytes = logFile.readBytes()
        val keepSize = KEEP_BYTES.coerceAtMost(bytes.size.toLong()).toInt()
        val keep = bytes.copyOfRange(bytes.size - keepSize, bytes.size)
        FileOutputStream(logFile, false).use { output ->
            output.write(
                "--- diagnostic journal trimmed; older lines were removed ---\n"
                    .toByteArray(StandardCharsets.UTF_8)
            )
            output.write(keep)
        }
    }

    private fun trimOlderEntriesLocked(now: Long) {
        if (!logFile.exists()) return
        val bytes = logFile.readBytes()
        val cutoff = now - MAX_AGE_MS
        val retained = ByteArrayOutputStream(bytes.size)
        var recordStart = -1
        var keepRecord = false
        var removedOldRecords = false
        var sawRecord = false
        var lineStart = 0

        while (lineStart < bytes.size) {
            var lineEnd = lineStart
            while (lineEnd < bytes.size && bytes[lineEnd] != '\n'.code.toByte()) lineEnd++
            val timestamp = parseEntryTimestamp(bytes, lineStart, lineEnd)
            if (timestamp != null) {
                if (recordStart >= 0) {
                    if (keepRecord) {
                        retained.write(bytes, recordStart, lineStart - recordStart)
                    } else {
                        removedOldRecords = true
                    }
                }
                recordStart = lineStart
                keepRecord = timestamp >= cutoff
                sawRecord = true
            }
            lineStart = (lineEnd + 1).coerceAtMost(bytes.size)
        }

        if (!sawRecord || !removedOldRecords && (recordStart < 0 || keepRecord)) return
        if (recordStart >= 0) {
            if (keepRecord) {
                retained.write(bytes, recordStart, bytes.size - recordStart)
            } else {
                removedOldRecords = true
            }
        }
        if (!removedOldRecords) return

        FileOutputStream(logFile, false).use { output ->
            output.write("--- diagnostic journal: entries older than 1 hour removed ---\n"
                .toByteArray(StandardCharsets.UTF_8))
            retained.writeTo(output)
        }
    }

    private fun parseEntryTimestamp(bytes: ByteArray, start: Int, end: Int): Long? {
        if (end - start < TIMESTAMP_PREFIX_LENGTH + 7) return null
        val marker = start + TIMESTAMP_PREFIX_LENGTH
        if (bytes[marker] != ' '.code.toByte() || bytes[marker + 2] != ' '.code.toByte() ||
            bytes[marker + 3] != 'p'.code.toByte() || bytes[marker + 4] != 'i'.code.toByte() ||
            bytes[marker + 5] != 'd'.code.toByte() || bytes[marker + 6] != '='.code.toByte()
        ) return null
        val prefix = String(bytes, start, TIMESTAMP_PREFIX_LENGTH, StandardCharsets.US_ASCII)
        val formatter = timeFormat.get() ?: SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss.SSS Z",
            Locale.US,
        ).also {
            it.timeZone = TimeZone.getDefault()
            timeFormat.set(it)
        }
        return runCatching { formatter.parse(prefix)?.time }.getOrNull()
    }

    private val journalEntryPattern = Regex("""^(.{29}) ([IWE]) pid=(\d+) process=(\S+) thread=(\S+) area=(\S+) \| (.*)$""")
    private val journalStatePattern = Regex(""" service\{[^}]*\} profileTest\{[^}]*\} auto\{[^}]*\} memory\{[^}]*\}""")
    private val journalAreas = mapOf(
        "app" to "Приложение", "diagnostic" to "Диагностика", "auto-vpn" to "Авто-VPN",
        "profile-test" to "Проверка профилей", "service" to "VPN-служба",
        "auto-vpn-services" to "Проверка доступности сайтов",
        "configuration-ui" to "Экран конфигураций", "ui" to "Интерфейс",
        "watchdog" to "Контроль зависания", "subscription" to "Подписки",
        "group-sync" to "Синхронизация групп", "update" to "Обновление", "crash" to "Сбой приложения",
    )
    private val journalEvents = mapOf(
        "process-start" to "Запуск приложения",
        "auto-search-session-start" to "Запущен поиск автопрофиля",
        "start-from-vpn-button" to "Запуск поиска кнопкой VPN",
        "direct-test-handoff-start" to "Подготовка сети к проверке профилей",
        "direct-search-service-start" to "Запущена служба поиска профилей",
        "detached-search-start" to "Начат поиск профилей",
        "detached-search-finish" to "Поиск профилей завершён",
        "candidate-test-start" to "Проверяется профиль",
        "candidate-test-success" to "Профиль прошёл проверку",
        "candidate-test-failed" to "Профиль не прошёл проверку",
        "candidate-test-cancelled" to "Проверка профиля отменена",
        "candidate-batch-cancelled" to "Пакет проверок отменён",
        "progress-clear" to "Счётчик проверки очищен",
        "progress-reset" to "Счётчик проверки сброшен",
        "progress-count" to "Обновлён счётчик проверки",
        "progress-skip" to "Профили пропущены фильтром",
        "progress-finish" to "Подсчёт проверки завершён",
        "post-connect-verification-scheduled" to "Запланирована проверка после подключения",
        "post-connect-attempt-start" to "Проверка после подключения началась",
        "post-connect-attempt-result" to "Результат проверки после подключения",
        "post-connect-verification-finished" to "Проверка после подключения завершена",
        "post-connect-country-check-finished" to "Проверена страна выхода",
        "gemini-rendered-probe" to "Проверка страницы Gemini",
        "gemini-probe-select" to "Выбран способ проверки Gemini",
        "gemini-probe-result" to "Результат HTTP-проверки Gemini",
        "active-monitor-start" to "Запущен контроль активного профиля",
        "active-monitor-tick" to "Плановая проверка активного профиля",
        "active-monitor-stop" to "Контроль активного профиля остановлен",
        "active-traffic-probe" to "Проверен трафик через VPN",
        "traffic-signal" to "Обнаружен подозрительный трафик",
        "exit-country-check" to "Проверена страна IP выхода",
        "network-status" to "Проверена доступность сети",
        "vpn-started-callback" to "VPN подключён",
        "vpn-stopped-callback" to "VPN отключён",
        "switchTo" to "Переключение профиля",
        "restart-scan-cycle" to "Начат новый круг поиска",
        "stop-scan" to "Поиск остановлен",
        "initial-scan-finish" to "Первичный поиск завершён",
        "main-thread-not-responding" to "Главный поток приложения не отвечает",
        "export-requested" to "Подготовлен экспорт журнала",
    )
    private val journalFields = mapOf(
        "id" to "ID", "profile" to "профиль", "group" to "группа", "type" to "тип",
        "name" to "имя", "kind" to "протокол", "address" to "сервер", "status" to "статус",
        "ping" to "пинг", "error" to "ошибка", "mode" to "режим", "attempt" to "попытка",
        "elapsedMs" to "время, мс", "delaysMs" to "интервалы, мс", "source" to "источник",
        "reason" to "причина", "total" to "всего", "finished" to "обработано", "working" to "рабочих",
        "pending" to "ожидает", "started" to "подключено", "enabled" to "включено", "running" to "выполняется",
        "progress" to "прогресс", "countryName" to "страна выхода", "country" to "код страны",
        "accepted" to "принят", "reachable" to "доступен", "bytesPerSecond" to "скорость, байт/с",
        "complete" to "загрузка завершена", "workingResult" to "профиль рабочий", "trigger" to "причина запуска",
        "urlTester" to "URL-проверка", "stillCurrent" to "тот же профиль", "autoRestarting" to "идёт автопереключение",
        "immediateVerifyActive" to "идёт проверка после подключения", "monitorActive" to "контроль активен",
        "main" to "главный процесс", "sdk" to "версия Android", "device" to "устройство",
        "pid" to "ID процесса", "process" to "процесс", "flavor" to "сборка", "version" to "версия",
        "selected" to "выбран профиль", "startedProfile" to "подключённый профиль", "mainStack" to "стек главного потока",
        "path" to "способ проверки", "renderRequested" to "проверка страницы запрошена",
        "finalUrl" to "конечный адрес", "state" to "результат классификации", "http" to "HTTP-код",
        "errorCode" to "код сетевой ошибки",
    )

    fun humanReadableHeader(): String = buildString {
        appendLine("Журнал диагностики ExclaveD")
        appendLine("Версия: " + BuildConfig.VERSION_NAME + " (" + BuildConfig.VERSION_CODE + "), " + flavorName())
        appendLine("Дата формирования: " + timestamp())
        appendLine("Устройство: " + oneLine(Build.MANUFACTURER + " " + Build.MODEL))
        appendLine("Android API: " + Build.VERSION.SDK_INT)
        appendLine("Текущее состояние: " + formatStateSummary(stateSummary()))
        appendLine()
        appendLine("События:")
        appendLine()
    }

    fun formatJournalForHuman(text: String): String {
        if (text.isEmpty()) return ""
        val lines = text.lineSequence().map { line ->
            val match = journalEntryPattern.matchEntire(line)
            when {
                match != null -> formatJournalEntry(match)
                line.startsWith("--- diagnostic journal trimmed") -> "Часть старых записей удалена для ограничения размера журнала."
                line.startsWith("--- diagnostic journal:") -> "Записи старше часа удалены."
                line.startsWith("--- beginning trimmed") -> "Показана последняя часть журнала."
                line.startsWith("Persistent diagnostic journal is empty") -> "Журнал пока пуст."
                line.startsWith("Persistent diagnostic journal read failed") -> "Не удалось прочитать журнал: " + translateErrorText(line)
                line.startsWith("    at ") || line.startsWith("\tat ") -> "    Вызов: " + line.trim()
                line.startsWith("Caused by:") -> "Причина: " + translateErrorText(line.removePrefix("Caused by:"))
                line.isBlank() -> ""
                else -> "    Подробности: " + translateErrorText(line)
            }
        }.joinToString("\n")
        return if (text.endsWith('\n')) lines + "\n" else lines
    }

    private fun formatJournalEntry(match: MatchResult): String {
        val messageRaw = match.groupValues[7]
        val stateMatch = journalStatePattern.find(messageRaw)
        val message = if (stateMatch == null) messageRaw else messageRaw.replace(stateMatch.value, "").trim()
        if (match.groupValues[6] == "app" && message.startsWith("initial-state ")) return ""
        val event = message.substringBefore(' ')
        val eventCode = event.takeIf { !it.contains('=') && (it.contains('-') || journalEvents.containsKey(it)) }
        val heading = eventCode?.let { journalEvents[it] ?: humanizeJournalEvent(it) } ?: "Событие"
        val detail = if (eventCode == null) message else message.removePrefix(event).trim()
        val severity = when (match.groupValues[2]) {
            "E" -> "ОШИБКА"
            "W" -> "ВНИМАНИЕ"
            else -> "ИНФО"
        }
        return buildString {
            append(match.groupValues[1])
            append(" | ")
            append(severity)
            append(" | ")
            append(journalAreas[match.groupValues[6]] ?: humanizeJournalEvent(match.groupValues[6]))
            append(" | ")
            append(heading)
            if (detail.isNotBlank()) append(": " + translateDiagnosticFields(detail))
            if (match.groupValues[2] != "I" && stateMatch != null) {
                append("\n    Состояние: ")
                append(formatStateSummary(stateMatch.value))
                append("\n    Процесс: ")
                append(match.groupValues[4])
                append("; поток: ")
                append(match.groupValues[5])
            }
        }
    }

    private fun formatStateSummary(raw: String): String {
        fun fields(section: String): Map<String, String> = Regex("([A-Za-z][A-Za-z0-9]*)=([^,}]*)").findAll(section)
            .associate { it.groupValues[1] to it.groupValues[2].trim() }
        val sections = mutableListOf<String>()
        Regex("service\\{([^}]*)\\}").find(raw)?.groupValues?.get(1)?.let {
            val data = fields(it)
            sections += "VPN " + if (data["started"] == "true") "подключён" else "отключён"
            sections += "профиль " + (data["startedProfile"] ?: "-")
        }
        Regex("profileTest\\{([^}]*)\\}").find(raw)?.groupValues?.get(1)?.let {
            val data = fields(it)
            sections += "проверка профилей " + (data["finished"] ?: "0") + "/" + (data["total"] ?: "0")
            sections += "рабочих " + (data["working"] ?: "0") + ", ожидают " + (data["pending"] ?: "0")
        }
        Regex("auto\\{([^}]*)\\}").find(raw)?.groupValues?.get(1)?.let {
            val data = fields(it)
            val mode = when (data["mode"]) { "1" -> "без RU"; "2" -> "Gemini"; else -> "обычный" }
            sections += "авторежим " + mode + " " + if (data["enabled"] == "true") "включён" else "выключен"
            sections += "кандидатов " + (data["working"] ?: "0")
            data["status"]?.takeIf { value -> value.isNotBlank() }?.let { status -> sections += "статус: " + status }
        }
        Regex("memory\\{([^}]*)\\}").find(raw)?.groupValues?.get(1)?.let {
            val data = fields(it)
            val used = data["used"]?.toLongOrNull() ?: 0L
            val max = data["max"]?.toLongOrNull() ?: 0L
            if (max > 0L) sections += "память " + formatBytes(used) + " из " + formatBytes(max)
        }
        return sections.joinToString("; ").ifBlank { "состояние не записано" }
    }

    private fun formatBytes(value: Long): String = when {
        value >= 1024L * 1024L -> (value / (1024L * 1024L)).toString() + " МБ"
        value >= 1024L -> (value / 1024L).toString() + " КБ"
        else -> value.toString() + " байт"
    }

    private fun translateDiagnosticFields(raw: String): String {
        val translated = Regex("([A-Za-z][A-Za-z0-9]*)=([^\\s,]+)").replace(raw) { match ->
            val key = match.groupValues[1]
            val rawValue = match.groupValues[2]
            val value = when {
                rawValue == "true" -> "да"
                rawValue == "false" -> "нет"
                key == "mode" -> when (rawValue) { "1" -> "без RU"; "2" -> "Gemini"; else -> "обычный" }
                key == "path" -> when (rawValue) {
                    "rendered-webview" -> "отрисованная страница"
                    "http" -> "HTTP-запрос"
                    "none" -> "не выполнялась"
                    else -> rawValue
                }
                key == "state" -> when (rawValue) {
                    "STARTED" -> "начата"
                    "CANCELLED" -> "отменена"
                    "AVAILABLE" -> "доступен"
                    "RESTRICTED" -> "региональное ограничение"
                    "CHALLENGE" -> "проверка Google"
                    "FORBIDDEN" -> "запрет 403"
                    "UNAVAILABLE" -> "недоступен"
                    "NO_RESPONSE" -> "нет ответа"
                    "UNKNOWN" -> "неясный ответ"
                    else -> rawValue
                }
                key == "reason" -> when {
                    rawValue.startsWith("dom-settled-") -> "страница загружена; " + when (rawValue.removePrefix("dom-settled-")) {
                        "no-known-gemini-marker" -> "нет известного признака доступности или блокировки"
                        "http-403" -> "ответ HTTP 403"
                        else -> rawValue.removePrefix("dom-settled-")
                    }
                    rawValue.startsWith("main-frame-error-") -> "ошибка основного кадра WebView, код ${rawValue.removePrefix("main-frame-error-")}"
                    rawValue.startsWith("exception-") -> "сетевая ошибка типа ${rawValue.removePrefix("exception-")}"
                    rawValue == "route-supported" -> "маршрут VPN поддерживает WebView"
                    rawValue == "render-not-requested" -> "проверка отрисованной страницы не запрошена"
                    rawValue == "service-mode-not-vpn" -> "служба работает не в режиме VPN"
                    rawValue == "socks-mode-enabled" -> "включён режим SOCKS"
                    rawValue == "system-tun-not-active" -> "системный туннель не используется"
                    rawValue == "app-excluded-from-vpn" -> "приложение исключено из VPN"
                    rawValue == "no-active-vpn-route" -> "нет активного маршрута VPN"
                    rawValue == "webview-load-start" -> "загрузка страницы начата"
                    rawValue == "webview-create-failed" -> "не удалось создать WebView"
                    rawValue == "dom-result-unavailable" -> "не удалось прочитать состояние страницы"
                    rawValue == "page-load-timeout" -> "истекло время загрузки страницы"
                    rawValue == "http-403-load-timeout" -> "HTTP 403; загрузка не завершилась"
                    rawValue == "http-403" -> "ответ HTTP 403"
                    rawValue == "google-sorry-url" -> "открыта страница Google Sorry"
                    rawValue == "google-challenge-marker" -> "обнаружен признак проверки Google"
                    rawValue == "regional-restriction-marker" -> "обнаружен признак регионального ограничения"
                    rawValue == "gemini-app-route" -> "открыт адрес приложения Gemini"
                    rawValue == "server-error" -> "ошибка сервера"
                    rawValue == "no-known-gemini-marker" -> "нет известного признака доступности или блокировки"
                    rawValue == "probe-cancelled" -> "проверка отменена"
                    rawValue == "render-process-gone" -> "процесс отрисовки WebView завершился"
                    rawValue == "load-url-failed" -> "не удалось открыть адрес"
                    rawValue == "classification-exception" -> "ошибка классификации результата"
                    else -> rawValue
                }
                key in setOf("source", "reason", "outcome", "trigger") -> humanizeJournalEvent(rawValue)
                else -> rawValue
            }
            (journalFields[key] ?: key) + ": " + value
        }
        return translateErrorText(translated)
    }

    private fun translateErrorText(raw: String): String {
        val phrases = listOf(
            "context deadline exceeded" to "истекло время ожидания",
            "TLS handshake timeout" to "истекло время TLS-согласования",
            "connection refused" to "сервер отказал в соединении",
            "no route to host" to "нет маршрута до сервера",
            "network is unreachable" to "сеть недоступна",
            "connection reset by peer" to "сервер сбросил соединение",
            "read/write on closed pipe" to "канал соединения закрыт",
            "unexpected EOF" to "соединение неожиданно завершилось",
            "socket timeout" to "истекло время ожидания ответа",
        )
        return phrases.fold(raw) { current, phrase -> current.replace(phrase.first, phrase.second, ignoreCase = true) }
    }

    private fun humanizeJournalEvent(value: String): String {
        journalEvents[value]?.let { return it }
        val words = mapOf(
            "start" to "запуск", "started" to "запущено", "stop" to "остановка", "stopped" to "остановлено",
            "finish" to "завершение", "finished" to "завершено", "check" to "проверка", "checking" to "проверяется",
            "test" to "проверка", "tested" to "проверено", "candidate" to "кандидат", "profile" to "профиль",
            "connected" to "подключено", "connect" to "подключение", "disconnect" to "отключение",
            "active" to "активный", "monitor" to "контроль", "tick" to "проход", "scan" to "поиск",
            "search" to "поиск", "progress" to "счётчик", "count" to "число", "reset" to "сброс",
            "clear" to "очистка", "country" to "страна", "exit" to "выход", "network" to "сеть",
            "traffic" to "трафик", "switch" to "переключение", "selected" to "выбран", "status" to "статус",
            "service" to "служба", "resume" to "продолжение", "cancel" to "отмена", "cancelled" to "отменено",
            "timeout" to "тайм-аут", "attempt" to "попытка", "result" to "результат", "error" to "ошибка",
            "warn" to "предупреждение", "retry" to "повтор", "direct" to "напрямую", "deferred" to "отложено",
            "failed" to "неудача", "success" to "успех", "replaced" to "заменено", "scheduled" to "запланировано",
            "available" to "доступно", "unavailable" to "недоступно", "ready" to "готово", "state" to "состояние",
            "initial" to "первичный", "manual" to "ручной", "auto" to "авто", "round" to "круг",
            "next" to "следующий", "unknown" to "неизвестно", "missing" to "отсутствует", "requested" to "запрошено",
            "watchdog" to "контроль", "export" to "экспорт", "read" to "чтение", "write" to "запись",
        )
        return value.split('-', '_').joinToString(" ") { words[it] ?: it }
    }

    private fun flavorName(): String = when (BuildConfig.FLAVOR) {
        "clone" -> "тестовая версия"
        "oss" -> "основная версия"
        else -> BuildConfig.FLAVOR
    }

    private fun timestamp(): String {
        val formatter = timeFormat.get() ?: SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss.SSS Z",
            Locale.US,
        ).also {
            it.timeZone = TimeZone.getDefault()
            timeFormat.set(it)
        }
        return formatter.format(Date())
    }

    private fun fileTimestamp(): String {
        val formatter = fileTimeFormat.get() ?: SimpleDateFormat(
            "yyyyMMdd-HHmmss",
            Locale.US,
        ).also {
            it.timeZone = TimeZone.getDefault()
            fileTimeFormat.set(it)
        }
        return formatter.format(Date())
    }

    private fun oneLine(value: String?, maxLength: Int = 240): String {
        return value.orEmpty()
            .replace('\r', ' ')
            .replace('\n', ' ')
            .replace('\t', ' ')
            .trim()
            .take(maxLength.coerceAtLeast(1))
    }
}
