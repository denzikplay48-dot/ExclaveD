/******************************************************************************
 * Copyright (C) 2021 by nekohasekai <contact-git@sekai.icu>                  *
 *                                                                            *
 * This program is free software: you can redistribute it and/or modify       *
 * it under the terms of the GNU General Public License as published by       *
 * the Free Software Foundation, either version 3 of the License, or          *
 *  (at your option) any later version.                                       *
 *                                                                            *
 * This program is distributed in the hope that it will be useful,            *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of             *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the              *
 * GNU General Public License for more details.                               *
 *                                                                            *
 * You should have received a copy of the GNU General Public License          *
 * along with this program. If not, see <http://www.gnu.org/licenses/>.       *
 *                                                                            *
 ******************************************************************************/

package io.nekohasekai.sagernet.ui

import android.content.Intent
import android.os.Build
import android.os.Bundle
import android.text.SpannableStringBuilder
import android.view.*
import androidx.appcompat.widget.Toolbar
import androidx.core.content.FileProvider
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.updatePadding
import io.nekohasekai.sagernet.BuildConfig
import io.nekohasekai.sagernet.LogLevel
import io.nekohasekai.sagernet.R
import io.nekohasekai.sagernet.SagerNet
import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.databinding.LayoutLogcatBinding
import io.nekohasekai.sagernet.ktx.*
import io.nekohasekai.sagernet.utils.ColorUtils
import io.nekohasekai.sagernet.utils.CrashHandler
import io.nekohasekai.sagernet.utils.DiagnosticLog
import java.io.BufferedReader
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStreamReader
import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets
import java.text.ParsePosition
import java.text.SimpleDateFormat
import java.util.Locale
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive

class LogcatFragment : ToolbarFragment(R.layout.layout_logcat),
    Toolbar.OnMenuItemClickListener {

    lateinit var binding: LayoutLogcatBinding
    private var streamJob: Job? = null
    private var logcatAgeTrimJob: Job? = null
    @Volatile private var logcatProcess: Process? = null
    private var diagnosticJournalSelected = false
    private var viewAvailable = false
    private var logcatScrollGeneration = 0L
    private var logcatUserScrollGeneration = 0L

    companion object {
        private const val MAX_VISIBLE_LOG_LINES = 2_000
        private const val MAX_VISIBLE_LOGCAT_LINES = 100
        private const val MAX_LOGCAT_BATCH_LINES = 100
        private const val LOGCAT_BATCH_INTERVAL_NS = 5_000_000_000L
        private const val MAX_INITIAL_JOURNAL_BYTES = 1024 * 1024
        private const val MAX_LOGCAT_AGE_MS = 10L * 60L * 1000L
        private const val LOGCAT_AGE_TRIM_INTERVAL_MS = 30_000L
        private val LOGCAT_TIMESTAMP_PREFIX = Regex("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} [+-]\\d{4}")
    }

    private val logcatTimestampFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z", Locale.US)

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)
        binding = LayoutLogcatBinding.bind(view)
        toolbar.setTitle(R.string.menu_log)
        binding.logsScrollView.setOnTouchListener { _, event ->
            if (event.action == MotionEvent.ACTION_DOWN || event.action == MotionEvent.ACTION_MOVE) {
                logcatUserScrollGeneration++
            }
            false
        }

        toolbar.inflateMenu(R.menu.logcat_menu)
        toolbar.setOnMenuItemClickListener(this)
        toolbar.menu.findItem(R.id.action_clear_logcat)?.isVisible = true

        ViewCompat.setOnApplyWindowInsetsListener(view.findViewById(R.id.logsScrollView)) { v, insets ->
            val bars = insets.getInsets(
                WindowInsetsCompat.Type.systemBars()
                        or WindowInsetsCompat.Type.displayCutout()
            )
            v.updatePadding(
                top = dp2px(8),
                left = bars.left + dp2px(8),
                right = bars.right + dp2px(8),
                bottom = bars.bottom + dp2px(64),
            )
            insets
        }

        (requireActivity() as? MainActivity)?.onBackPressedCallback?.isEnabled = true

        viewAvailable = true
        startLogcatStream()
    }

    override fun onDestroyView() {
        viewAvailable = false
        stopLogStream()
        super.onDestroyView()
    }

    override fun onResume() {
        super.onResume()
        updateJournalMenuTitle()
        if (DataStore.logLevel == LogLevel.NONE && diagnosticJournalSelected) startLogcatStream()
    }

    private fun updateJournalMenuTitle() {
        val diagnosticAvailable = DataStore.logLevel != LogLevel.NONE
        toolbar.menu.findItem(R.id.action_toggle_diagnostic)?.apply {
            title = getString(if (diagnosticJournalSelected) R.string.view_logcat else R.string.view_diagnostic_journal)
            isChecked = diagnosticJournalSelected
            isVisible = diagnosticAvailable
        }
        toolbar.menu.findItem(R.id.action_send_diagnostic_log)?.isVisible = diagnosticAvailable
        toolbar.menu.findItem(R.id.action_clear_logcat)?.isVisible = !diagnosticJournalSelected
    }

    private fun stopLogStream() {
        logcatScrollGeneration++
        val job = streamJob
        val process = logcatProcess
        logcatProcess = null
        streamJob = null
        job?.cancel()
        process?.destroy()
        logcatAgeTrimJob?.cancel()
        logcatAgeTrimJob = null
    }

    private fun appendLogUpdate(chunk: CharSequence) {
        if (chunk.isEmpty()) return
        val scrollView = binding.logsScrollView
        val textView = binding.logsTextView
        val oldScrollY = scrollView.scrollY
        val wasAtBottom = scrollView.childCount == 0 ||
                scrollView.getChildAt(0).bottom <= oldScrollY + scrollView.height + dp2px(24)
        val combined = SpannableStringBuilder(textView.text).append(chunk)
        if (!diagnosticJournalSelected) {
            val ageTrimOffset = logcatAgeTrimOffset(combined)
            if (ageTrimOffset > 0) combined.delete(0, ageTrimOffset)
        }
        val excessLines = combined.count { it == '\n' } - MAX_VISIBLE_LOG_LINES
        var trimOffset = 0
        if (excessLines > 0) {
            var removedLines = 0
            for (index in 0 until combined.length) {
                if (combined[index] == '\n') {
                    removedLines++
                    if (removedLines == excessLines) {
                        trimOffset = index + 1
                        break
                    }
                }
            }
        }
        if (trimOffset > 0) combined.delete(0, trimOffset)
        textView.text = combined
        scrollView.post {
            if (!viewAvailable) return@post
            if (wasAtBottom) {
                scrollView.fullScroll(View.FOCUS_DOWN)
            } else {
                val maxScroll = (scrollView.getChildAt(0).height - scrollView.height).coerceAtLeast(0)
                scrollView.scrollTo(0, oldScrollY.coerceAtMost(maxScroll))
            }
        }
    }

    private fun appendLogcatUpdate(chunk: CharSequence) {
        if (chunk.isEmpty()) return
        val scrollView = binding.logsScrollView
        val textView = binding.logsTextView
        val oldScrollY = scrollView.scrollY
        val wasAtBottom = scrollView.childCount == 0 || !scrollView.canScrollVertically(1)
        val lineHeight = textView.lineHeight.coerceAtLeast(1)
        var removedLines = 0

        val combined = SpannableStringBuilder(textView.text).append(chunk)
        val ageTrimOffset = logcatAgeTrimOffset(combined)
        if (ageTrimOffset > 0) {
            removedLines += combined.subSequence(0, ageTrimOffset).count { it == '\n' }
            combined.delete(0, ageTrimOffset)
        }
        val lineCount = combined.count { it == '\n' } +
                if (combined.isNotEmpty() && combined.last() != '\n') 1 else 0
        val excessLines = lineCount - MAX_VISIBLE_LOGCAT_LINES
        if (excessLines > 0) {
            var trimOffset = 0
            var trimmedCount = 0
            for (index in 0 until combined.length) {
                if (combined[index] == '\n') {
                    trimmedCount++
                    if (trimmedCount == excessLines) {
                        trimOffset = index + 1
                        break
                    }
                }
            }
            if (trimOffset > 0) {
                combined.delete(0, trimOffset)
                removedLines += trimmedCount
            }
        }
        textView.text = combined
        scheduleLogcatScroll(scrollView, oldScrollY, wasAtBottom, removedLines, lineHeight)
    }

    private fun scheduleLogcatScroll(
        scrollView: androidx.core.widget.NestedScrollView,
        oldScrollY: Int,
        wasAtBottom: Boolean,
        removedLines: Int,
        lineHeight: Int,
    ) {
        val generation = ++logcatScrollGeneration
        val userScrollGeneration = logcatUserScrollGeneration
        scrollView.post {
            if (!viewAvailable || generation != logcatScrollGeneration ||
                userScrollGeneration != logcatUserScrollGeneration
            ) return@post
            if (wasAtBottom) {
                scrollView.fullScroll(View.FOCUS_DOWN)
            } else if (removedLines > 0) {
                val maxScroll = (scrollView.getChildAt(0).height - scrollView.height).coerceAtLeast(0)
                val preservedScrollY = (oldScrollY - removedLines * lineHeight).coerceAtLeast(0)
                scrollView.scrollTo(0, preservedScrollY.coerceAtMost(maxScroll))
            }
        }
    }

    private fun logcatAgeTrimOffset(text: CharSequence): Int {
        val cutoff = System.currentTimeMillis() - MAX_LOGCAT_AGE_MS
        var lineStart = 0
        while (lineStart < text.length) {
            var lineEnd = lineStart
            while (lineEnd < text.length && text[lineEnd] != '\n') lineEnd++
            val line = text.subSequence(lineStart, lineEnd).toString()
            val timestamp = if (LOGCAT_TIMESTAMP_PREFIX.containsMatchIn(line)) {
                logcatTimestampFormat.parse(line, ParsePosition(0))?.time
            } else {
                null
            }
            if (timestamp != null && timestamp >= cutoff) return lineStart
            if (lineEnd == text.length) break
            lineStart = lineEnd + 1
        }
        return text.length
    }

    private fun startLogcatAgeTrim() {
        logcatAgeTrimJob?.cancel()
        logcatAgeTrimJob = runOnDefaultDispatcher {
            while (isActive && viewAvailable && !diagnosticJournalSelected) {
                delay(LOGCAT_AGE_TRIM_INTERVAL_MS)
                onMainDispatcher {
                    if (viewAvailable && !diagnosticJournalSelected) trimVisibleLogcatByAge()
                }
            }
        }
    }

    private fun trimVisibleLogcatByAge() {
        val scrollView = binding.logsScrollView
        val textView = binding.logsTextView
        val oldScrollY = scrollView.scrollY
        val wasAtBottom = scrollView.childCount == 0 || !scrollView.canScrollVertically(1)
        val trimmed = SpannableStringBuilder(textView.text)
        val trimOffset = logcatAgeTrimOffset(trimmed)
        if (trimOffset <= 0) return
        val removedLines = trimmed.subSequence(0, trimOffset).count { it == '\n' }
        trimmed.delete(0, trimOffset)
        textView.text = trimmed
        scheduleLogcatScroll(
            scrollView,
            oldScrollY,
            wasAtBottom,
            removedLines,
            textView.lineHeight.coerceAtLeast(1),
        )
    }

    private fun startLogcatStream() {
        stopLogStream()
        diagnosticJournalSelected = false
        updateJournalMenuTitle()
        binding.logsTextView.text = ""
        binding.logsScrollView.scrollTo(0, 0)
        startLogcatAgeTrim()
        streamJob = runOnIoDispatcher {
            try {
                streamingLog()
            } catch (e: IOException) {
                if (isActive && viewAvailable && !diagnosticJournalSelected) {
                    DiagnosticLog.warn("logcat-view", "logcat stream stopped unexpectedly", e)
                }
            }
        }
    }

    private fun startDiagnosticJournalStream() {
        if (DataStore.logLevel == LogLevel.NONE) {
            startLogcatStream()
            return
        }
        stopLogStream()
        diagnosticJournalSelected = true
        updateJournalMenuTitle()
        binding.logsTextView.text = getString(R.string.diagnostic_journal_empty)
        streamJob = runOnIoDispatcher { streamingDiagnosticJournal() }
    }

    private suspend fun streamingLog() = onIoDispatcher {
        val process = try {
            ProcessBuilder(
                listOf("logcat",
                    "-T", MAX_VISIBLE_LOGCAT_LINES.toString(),
                    "-v", if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                        "threadtime,year,zone,color"
                    } else {
                        "threadtime,year,zone"
                    },
                    "-s", arrayOf(
                        "AndroidRuntime:D",
                        "ProxyInstance:D",
                        "GuardedProcessPool:D",
                        "VpnService:D",
                        "Go:D",
                        "v2ray-core:D",
                        "libsagernetcore:D",
                        "libnaive:D",
                        "libshadowquic:D",
                        "Exclave:D",
                        "*:S",
                    ).joinToString(",")
                )
            ).start()
        } catch (_: Exception) {
            return@onIoDispatcher
        }
        logcatProcess = process
        val stdout = BufferedReader(InputStreamReader(process.inputStream))
        val bufferedLogLines = arrayListOf<String>()
        var timeLastNotify = System.nanoTime()
        var firstBatch = true
        try {
            while (true) {
                val line = try {
                    stdout.readLine()
                } catch (e: IOException) {
                    if (isActive && viewAvailable && !diagnosticJournalSelected) {
                        DiagnosticLog.warn("logcat-view", "logcat stream read failed", e)
                    }
                    null
                } ?: break
                bufferedLogLines.add(line)
                if (bufferedLogLines.size > MAX_LOGCAT_BATCH_LINES) bufferedLogLines.removeAt(0)
                val timeNow = System.nanoTime()
                val elapsedSinceUpdate = timeNow - timeLastNotify
                if (firstBatch) {
                    if (bufferedLogLines.size < MAX_LOGCAT_BATCH_LINES &&
                        elapsedSinceUpdate < LOGCAT_BATCH_INTERVAL_NS
                    ) continue
                } else if (elapsedSinceUpdate < LOGCAT_BATCH_INTERVAL_NS) {
                    continue
                }

                timeLastNotify = timeNow
                firstBatch = false

                onMainDispatcher {
                    if (!viewAvailable || diagnosticJournalSelected) return@onMainDispatcher
                    appendLogcatUpdate(
                        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                            ColorUtils.ansiEscapeToSpannable(binding.root.context,
                                bufferedLogLines.joinToString(separator = "\n", postfix = "\n")
                            )
                        } else {
                            bufferedLogLines.joinToString(separator = "\n", postfix = "\n")
                        }
                    )
                    bufferedLogLines.clear()
                }
            }
        } finally {
            process?.destroy()
            if (logcatProcess === process) logcatProcess = null
        }
    }

    private suspend fun streamingDiagnosticJournal() = onIoDispatcher {
        val file = DiagnosticLog.logFile
        var offset = 0L
        var firstRead = true
        while (isActive && diagnosticJournalSelected && DataStore.logLevel != LogLevel.NONE) {
            if (file.exists()) {
                val length = file.length()
                if (firstRead || length < offset) {
                    val end = length
                    val start = (end - MAX_INITIAL_JOURNAL_BYTES).coerceAtLeast(0L)
                    val bytes = runCatching { RandomAccessFile(file, "r").use { journal ->
                        journal.seek(start)
                        ByteArray((end - start).toInt()).also { journal.readFully(it) }
                    } }.getOrNull()
                    if (bytes == null) {
                        delay(750L)
                        continue
                    }
                    var snapshot = String(bytes, StandardCharsets.UTF_8)
                    if (start > 0L) snapshot = snapshot.substringAfter('\n', "")
                    snapshot = snapshot.lines().takeLast(MAX_VISIBLE_LOG_LINES)
                        .joinToString(separator = "\n", postfix = "\n")
                    offset = end
                    firstRead = false
                    onMainDispatcher {
                        if (viewAvailable && diagnosticJournalSelected) {
                            val readableSnapshot = if (snapshot.isBlank()) {
                                getString(R.string.diagnostic_journal_empty)
                            } else {
                                DiagnosticLog.formatJournalForHuman(snapshot)
                            }
                            binding.logsTextView.text = DiagnosticLog.humanReadableHeader() +
                                    readableSnapshot
                            binding.logsScrollView.post { binding.logsScrollView.fullScroll(View.FOCUS_DOWN) }
                        }
                    }
                } else if (length > offset) {
                    val end = length
                    val bytes = runCatching { RandomAccessFile(file, "r").use { journal ->
                        journal.seek(offset)
                        ByteArray((end - offset).toInt()).also { journal.readFully(it) }
                    } }.getOrNull()
                    if (bytes == null) {
                        delay(750L)
                        continue
                    }
                    offset = end
                    val chunk = String(bytes, StandardCharsets.UTF_8)
                    onMainDispatcher {
                        if (!viewAvailable || !diagnosticJournalSelected) return@onMainDispatcher
                        appendLogUpdate(DiagnosticLog.formatJournalForHuman(chunk))
                    }
                }
            }
            delay(750L)
        }
    }

    override fun onMenuItemClick(item: MenuItem): Boolean {
        when (item.itemId) {
            R.id.action_toggle_diagnostic -> {
                if (DataStore.logLevel == LogLevel.NONE || diagnosticJournalSelected) {
                    startLogcatStream()
                } else {
                    startDiagnosticJournalStream()
                }
            }
            R.id.action_clear_logcat -> {
                runOnIoDispatcher {
                    val command = listOf("logcat", "-c")
                    val process = ProcessBuilder(command).start()
                    process.waitFor()
                    onMainDispatcher {
                        binding.logsTextView.text = ""
                    }
                }
            }
            R.id.action_send_logcat -> {
                val context = requireContext()
                runOnDefaultDispatcher {
                    val logFile = File.createTempFile("Exclave ",
                        ".log",
                        File(app.externalCacheDir, "log").also { it.mkdirs() })

                    var report = CrashHandler.buildReportHeader()

                    report += "Logcat: \n\n"

                    logFile.writeText(report)

                    try {
                        Runtime.getRuntime().exec(arrayOf("logcat", "-d")).inputStream.use(
                            FileOutputStream(
                                logFile, true
                            )
                        )
                    } catch (e: IOException) {
                        Logs.w(e)
                        logFile.appendText("Export logcat error: " + CrashHandler.formatThrowable(e))
                    }

                    context.startActivity(
                        Intent.createChooser(
                            Intent(Intent.ACTION_SEND).setType("text/plain")
                                .setFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                                .putExtra(
                                    Intent.EXTRA_STREAM, FileProvider.getUriForFile(
                                        context, BuildConfig.APPLICATION_ID + ".cache", logFile
                                    )
                                ), context.getString(androidx.appcompat.R.string.abc_shareactionprovider_share_with)
                        )
                    )
                }
            }
            R.id.action_send_diagnostic_log -> {
                if (DataStore.logLevel == LogLevel.NONE) return true
                runOnDefaultDispatcher {
                    val logFile = DiagnosticLog.createShareFile()
                    onMainDispatcher {
                        if (!isAdded) return@onMainDispatcher
                        val context = requireContext()
                        context.startActivity(
                            Intent.createChooser(
                                Intent(Intent.ACTION_SEND).setType("text/plain")
                                    .setFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                                    .putExtra(
                                        Intent.EXTRA_STREAM, FileProvider.getUriForFile(
                                            context, BuildConfig.APPLICATION_ID + ".cache", logFile
                                        )
                                    ), context.getString(androidx.appcompat.R.string.abc_shareactionprovider_share_with)
                            )
                        )
                    }
                }
            }
        }
        return true
    }
}
