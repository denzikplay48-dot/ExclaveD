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
        target.writeText(buildExportHeader(), StandardCharsets.UTF_8)
        FileOutputStream(target, true).use { output ->
            output.write(readRecentText(MAX_BYTES.toInt()).toByteArray(StandardCharsets.UTF_8))
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

        if (!sawRecord || (!removedOldRecords && (recordStart < 0 || keepRecord))) return
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

    private fun buildExportHeader(): String {
        return buildString {
            appendLine("Exclave Diagnostic Journal")
            appendLine("Version: ${BuildConfig.VERSION_NAME} (${BuildConfig.VERSION_CODE}) ${BuildConfig.FLAVOR}")
            appendLine("Date: ${timestamp()}")
            appendLine("Process: $processName")
            appendLine("SDK: ${Build.VERSION.SDK_INT}")
            appendLine("Device: ${oneLine("${Build.MANUFACTURER} ${Build.MODEL}")}")
            appendLine("State: ${stateSummary()}")
            appendLine()
            appendLine("Journal:")
            appendLine()
        }
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
