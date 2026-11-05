/*
 * Copyright (C) 2026
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package io.nekohasekai.sagernet.bg

import android.os.Handler
import android.os.Looper
import android.os.SystemClock
import android.view.ContextThemeWrapper
import android.view.ViewGroup
import android.webkit.WebResourceError
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebView
import android.webkit.WebViewClient
import io.nekohasekai.sagernet.R
import io.nekohasekai.sagernet.SagerNet
import io.nekohasekai.sagernet.utils.DiagnosticLog
import kotlinx.coroutines.suspendCancellableCoroutine
import org.json.JSONArray
import org.json.JSONObject
import java.net.URL
import java.net.HttpURLConnection
import kotlin.coroutines.resume

internal object GeminiRenderedPageProbe {
    private const val PAGE_URL = "https://gemini.google.com/"
    private const val LOAD_TIMEOUT_MS = 15_000L
    private const val DOM_SETTLE_TIMEOUT_MS = 5_000L
    private const val DOM_POLL_INTERVAL_MS = 500L
    private const val DOM_MARKER_SCRIPT = """(function(){
        var text=(document.body&&document.body.innerText||'').toLowerCase();
        var region=['not supported in your country','not currently supported in your country',
            'not available in your country','not yet available in your country',
            "isn't supported in your country","isn't currently supported in your country",
            "isn't available in your country",'not supported in your region',
            'not available in your region','не поддерживается в вашей стране',
            'недоступен в вашей стране','недоступна в вашей стране'];
        var challenge=['unusual traffic','automated queries','our systems have detected','not a robot'];
        return JSON.stringify({url:location.href,
            region:region.some(function(marker){return text.indexOf(marker)>=0;}),
            challenge:challenge.some(function(marker){return text.indexOf(marker)>=0;})});
    })()"""

    suspend fun check(): ServiceAvailabilityProbe.Result = suspendCancellableCoroutine { continuation ->
        val handler = Handler(Looper.getMainLooper())
        val startedAt = SystemClock.elapsedRealtime()
        var webView: WebView? = null
        var lastUrl = PAGE_URL
        var mainFrameStatus: Int? = null
        var pageCommittedAt = 0L
        var pageFinishedAt = 0L
        var domStartedAt = 0L
        var domSnapshotRead = false
        var completed = false

        DiagnosticLog.event(
            "auto-vpn-services",
            "gemini-rendered-probe path=rendered-webview state=STARTED http=none " +
                    "url=${safeLocation(PAGE_URL)} reason=webview-load-start",
        )

        lateinit var pollDom: Runnable
        lateinit var timeout: Runnable
        lateinit var domSettleTimeout: Runnable

        fun destroyWebView() {
            handler.removeCallbacks(pollDom)
            handler.removeCallbacks(timeout)
            handler.removeCallbacks(domSettleTimeout)
            webView?.let { view ->
                webView = null
                runCatching { (view.parent as? ViewGroup)?.removeView(view) }
                runCatching { view.stopLoading() }
                runCatching { view.destroy() }
            }
        }

        fun finish(state: ServiceAvailabilityProbe.State, reason: String) {
            if (completed) return
            completed = true
            val code = mainFrameStatus ?: if (pageCommittedAt > 0L || pageFinishedAt > 0L) {
                HttpURLConnection.HTTP_OK
            } else {
                null
            }
            val location = safeLocation(lastUrl)
            val elapsed = (SystemClock.elapsedRealtime() - startedAt).coerceAtLeast(0L)
            DiagnosticLog.event(
                "auto-vpn-services",
                "gemini-rendered-probe path=rendered-webview state=${state.name} " +
                        "http=${code ?: "none"} url=${location.ifBlank { "unknown" }} " +
                        "reason=$reason elapsedMs=$elapsed",
            )
            destroyWebView()
            if (continuation.isActive) {
                continuation.resume(
                    ServiceAvailabilityProbe.Result(
                        service = ServiceAvailabilityProbe.Service.GEMINI,
                        state = state,
                        responseCode = code,
                        elapsedMillis = elapsed,
                        finalLocation = location,
                    ),
                )
            }
        }

        fun classify(url: String, code: Int, markers: String = "") = runCatching {
            ServiceAvailabilityProbe.classifyGeminiResponseDetailed(URL(url), code, markers)
        }.getOrElse {
            ServiceAvailabilityProbe.GeminiClassification(
                ServiceAvailabilityProbe.State.UNKNOWN,
                "classification-exception",
            )
        }

        fun isConclusive(state: ServiceAvailabilityProbe.State): Boolean = when (state) {
            ServiceAvailabilityProbe.State.AVAILABLE,
            ServiceAvailabilityProbe.State.UNAVAILABLE,
            ServiceAvailabilityProbe.State.RESTRICTED,
            ServiceAvailabilityProbe.State.CHALLENGE,
            ServiceAvailabilityProbe.State.FORBIDDEN -> true
            else -> false
        }

        fun effectiveCode(): Int = mainFrameStatus ?: if (pageCommittedAt > 0L || pageFinishedAt > 0L) {
            HttpURLConnection.HTTP_OK
        } else {
            0
        }

        fun startDomInspection() {
            if (domStartedAt == 0L) {
                domStartedAt = SystemClock.elapsedRealtime()
            }
            handler.removeCallbacks(pollDom)
            handler.post(pollDom)
        }

        fun scheduleDomSettleTimeout() {
            handler.removeCallbacks(domSettleTimeout)
            handler.postDelayed(domSettleTimeout, DOM_SETTLE_TIMEOUT_MS)
        }

        fun inspectDom(rawValue: String?) {
            if (completed) return
            val encoded = runCatching { JSONArray().put(rawValue).getString(0) }.getOrNull()
            val snapshot = encoded?.let { runCatching { JSONObject(it) }.getOrNull() }
            if (snapshot != null) {
                domSnapshotRead = true
                lastUrl = snapshot.optString("url", lastUrl)
                val markerText = buildString {
                    if (snapshot.optBoolean("region")) append("not supported in your country ")
                    if (snapshot.optBoolean("challenge")) append("unusual traffic ")
                }
                val classification = classify(lastUrl, effectiveCode(), markerText)
                if (isConclusive(classification.state)) {
                    finish(classification.state, classification.reason)
                    return
                }
            }
            handler.postDelayed(pollDom, DOM_POLL_INTERVAL_MS)
        }

        pollDom = Runnable {
            if (completed) return@Runnable
            val view = webView ?: return@Runnable
            runCatching {
                view.evaluateJavascript(DOM_MARKER_SCRIPT) { value -> inspectDom(value) }
            }.onFailure {
                if (domStartedAt == 0L) startDomInspection()
            }
        }

        domSettleTimeout = Runnable {
            if (completed) return@Runnable
            val classification = classify(lastUrl, effectiveCode())
            if (isConclusive(classification.state)) {
                finish(classification.state, classification.reason)
            } else {
                finish(
                    ServiceAvailabilityProbe.State.UNKNOWN,
                    if (domSnapshotRead) classification.reason else "dom-result-unavailable",
                )
            }
        }

        timeout = Runnable {
            if (completed) return@Runnable
            val classification = classify(lastUrl, effectiveCode())
            if (isConclusive(classification.state)) {
                finish(classification.state, classification.reason)
            } else {
                finish(
                    ServiceAvailabilityProbe.State.UNKNOWN,
                    if (!domSnapshotRead && (pageCommittedAt > 0L || pageFinishedAt > 0L)) {
                        "dom-result-unavailable"
                    } else {
                        "page-load-timeout"
                    },
                )
            }
        }

        continuation.invokeOnCancellation {
            handler.post {
                if (!completed) {
                    completed = true
                    DiagnosticLog.event(
                        "auto-vpn-services",
                        "gemini-rendered-probe path=rendered-webview state=CANCELLED http=${mainFrameStatus ?: "none"} " +
                                "url=${safeLocation(lastUrl)} reason=probe-cancelled",
                    )
                    destroyWebView()
                }
            }
        }

        handler.post {
            if (!continuation.isActive || completed) return@post
            val view = runCatching {
                WebView(ContextThemeWrapper(SagerNet.application, R.style.Theme_SagerNet))
            }.getOrElse {
                finish(ServiceAvailabilityProbe.State.UNKNOWN, "webview-create-failed")
                return@post
            }
            webView = view
            view.settings.apply {
                javaScriptEnabled = true
                domStorageEnabled = true
                loadsImagesAutomatically = false
                blockNetworkImage = true
                cacheMode = android.webkit.WebSettings.LOAD_DEFAULT
                allowFileAccess = false
                allowContentAccess = false
                javaScriptCanOpenWindowsAutomatically = false
                setSupportMultipleWindows(false)
                mixedContentMode = android.webkit.WebSettings.MIXED_CONTENT_NEVER_ALLOW
            }
            view.webViewClient = object : WebViewClient() {
                override fun onPageStarted(view: WebView, url: String, favicon: android.graphics.Bitmap?) {
                    lastUrl = url
                    mainFrameStatus = null
                    pageCommittedAt = 0L
                    pageFinishedAt = 0L
                    domStartedAt = 0L
                    domSnapshotRead = false
                    handler.removeCallbacks(pollDom)
                    handler.removeCallbacks(domSettleTimeout)
                    val classification = classify(url, 0)
                    if (isConclusive(classification.state)) finish(classification.state, classification.reason)
                }

                override fun onPageCommitVisible(view: WebView, url: String) {
                    lastUrl = url
                    pageCommittedAt = SystemClock.elapsedRealtime()
                    val classification = classify(url, effectiveCode())
                    if (isConclusive(classification.state)) {
                        finish(classification.state, classification.reason)
                    } else {
                        startDomInspection()
                    }
                }

                override fun onPageFinished(view: WebView, url: String) {
                    lastUrl = url
                    if (pageFinishedAt == 0L) pageFinishedAt = SystemClock.elapsedRealtime()
                    scheduleDomSettleTimeout()
                    val classification = classify(url, effectiveCode())
                    if (isConclusive(classification.state)) {
                        finish(classification.state, classification.reason)
                    } else {
                        startDomInspection()
                    }
                }

                override fun onReceivedHttpError(
                    view: WebView,
                    request: WebResourceRequest,
                    errorResponse: WebResourceResponse,
                ) {
                    if (request.isForMainFrame) {
                        lastUrl = request.url.toString()
                        mainFrameStatus = errorResponse.statusCode
                        val classification = classify(lastUrl, errorResponse.statusCode)
                        if (isConclusive(classification.state)) {
                            finish(classification.state, classification.reason)
                        }
                    }
                }

                override fun onReceivedError(
                    view: WebView,
                    request: WebResourceRequest,
                    error: WebResourceError,
                ) {
                    if (request.isForMainFrame) {
                        finish(ServiceAvailabilityProbe.State.UNKNOWN, "main-frame-error-${error.errorCode}")
                    }
                }

                override fun onRenderProcessGone(view: WebView, detail: android.webkit.RenderProcessGoneDetail): Boolean {
                    finish(ServiceAvailabilityProbe.State.UNKNOWN, "render-process-gone")
                    return true
                }
            }
            handler.postDelayed(timeout, LOAD_TIMEOUT_MS)
            runCatching { view.loadUrl(PAGE_URL) }
                .onFailure { finish(ServiceAvailabilityProbe.State.NO_RESPONSE, "load-url-failed") }
        }
    }

    private fun safeLocation(value: String): String = runCatching {
        URL(value).let { "${it.protocol}://${it.host}${it.path}" }
    }.getOrDefault("")
}
