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
    private const val MAX_TEXT_LENGTH = 4_096

    suspend fun check(): ServiceAvailabilityProbe.Result = suspendCancellableCoroutine { continuation ->
        val handler = Handler(Looper.getMainLooper())
        val startedAt = SystemClock.elapsedRealtime()
        var webView: WebView? = null
        var lastUrl = PAGE_URL
        var mainFrameStatus: Int? = null
        var pageFinishedAt = 0L
        var completed = false

        lateinit var pollDom: Runnable
        lateinit var timeout: Runnable

        fun destroyWebView() {
            handler.removeCallbacks(pollDom)
            handler.removeCallbacks(timeout)
            webView?.let { view ->
                webView = null
                runCatching { (view.parent as? ViewGroup)?.removeView(view) }
                runCatching { view.stopLoading() }
                runCatching { view.destroy() }
            }
        }

        fun finish(state: ServiceAvailabilityProbe.State) {
            if (completed) return
            completed = true
            val code = mainFrameStatus ?: if (pageFinishedAt > 0L) HttpURLConnection.HTTP_OK else null
            val location = safeLocation(lastUrl)
            val elapsed = (SystemClock.elapsedRealtime() - startedAt).coerceAtLeast(0L)
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

        fun inspectDom(rawValue: String?) {
            if (completed) return
            val encoded = runCatching { JSONArray().put(rawValue).getString(0) }.getOrNull()
            val snapshot = encoded?.let { runCatching { JSONObject(it) }.getOrNull() }
            if (snapshot != null) {
                lastUrl = snapshot.optString("url", lastUrl)
                val text = snapshot.optString("text").take(MAX_TEXT_LENGTH)
                val effectiveCode = mainFrameStatus ?: HttpURLConnection.HTTP_OK
                val state = runCatching {
                    ServiceAvailabilityProbe.classifyGeminiResponse(URL(lastUrl), effectiveCode, text)
                }.getOrDefault(ServiceAvailabilityProbe.State.UNKNOWN)
                if (state == ServiceAvailabilityProbe.State.AVAILABLE ||
                    state == ServiceAvailabilityProbe.State.RESTRICTED ||
                    state == ServiceAvailabilityProbe.State.CHALLENGE
                ) {
                    finish(state)
                    return
                }
                if (pageFinishedAt > 0L && SystemClock.elapsedRealtime() - pageFinishedAt >= DOM_SETTLE_TIMEOUT_MS) {
                    finish(state)
                    return
                }
            } else if (pageFinishedAt > 0L &&
                SystemClock.elapsedRealtime() - pageFinishedAt >= DOM_SETTLE_TIMEOUT_MS
            ) {
                finish(ServiceAvailabilityProbe.State.UNKNOWN)
                return
            }
            handler.postDelayed(pollDom, DOM_POLL_INTERVAL_MS)
        }

        pollDom = Runnable {
            if (completed) return@Runnable
            val view = webView ?: return@Runnable
            runCatching {
                view.evaluateJavascript(
                    "JSON.stringify({url:location.href,text:(document.body&&document.body.innerText||'').slice(0,$MAX_TEXT_LENGTH)})",
                    { value -> inspectDom(value) },
                )
            }.onFailure {
                if (pageFinishedAt > 0L) inspectDom(null)
            }
        }

        timeout = Runnable {
            if (completed) return@Runnable
            finish(
                if (mainFrameStatus == HttpURLConnection.HTTP_FORBIDDEN) {
                    ServiceAvailabilityProbe.State.FORBIDDEN
                } else {
                    ServiceAvailabilityProbe.State.NO_RESPONSE
                },
            )
        }

        continuation.invokeOnCancellation {
            handler.post {
                if (!completed) {
                    completed = true
                    destroyWebView()
                }
            }
        }

        handler.post {
            if (!continuation.isActive || completed) return@post
            val view = runCatching {
                WebView(ContextThemeWrapper(SagerNet.application, R.style.Theme_SagerNet))
            }.getOrElse {
                finish(ServiceAvailabilityProbe.State.UNKNOWN)
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
                }

                override fun onPageFinished(view: WebView, url: String) {
                    lastUrl = url
                    if (pageFinishedAt == 0L) pageFinishedAt = SystemClock.elapsedRealtime()
                    handler.removeCallbacks(pollDom)
                    handler.post(pollDom)
                }

                override fun onReceivedHttpError(
                    view: WebView,
                    request: WebResourceRequest,
                    errorResponse: WebResourceResponse,
                ) {
                    if (request.isForMainFrame) {
                        lastUrl = request.url.toString()
                        mainFrameStatus = errorResponse.statusCode
                    }
                }

                override fun onReceivedError(
                    view: WebView,
                    request: WebResourceRequest,
                    error: WebResourceError,
                ) {
                    if (request.isForMainFrame) finish(ServiceAvailabilityProbe.State.NO_RESPONSE)
                }

                override fun onRenderProcessGone(view: WebView, detail: android.webkit.RenderProcessGoneDetail): Boolean {
                    finish(ServiceAvailabilityProbe.State.UNKNOWN)
                    return true
                }
            }
            handler.postDelayed(timeout, LOAD_TIMEOUT_MS)
            runCatching { view.loadUrl(PAGE_URL) }
                .onFailure { finish(ServiceAvailabilityProbe.State.NO_RESPONSE) }
        }
    }

    private fun safeLocation(value: String): String = runCatching {
        URL(value).let { "${it.protocol}://${it.host}${it.path}" }
    }.getOrDefault("")
}
