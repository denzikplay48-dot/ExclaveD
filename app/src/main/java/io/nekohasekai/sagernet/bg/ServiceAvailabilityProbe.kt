/*
 * Copyright (C) 2026
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package io.nekohasekai.sagernet.bg

import io.nekohasekai.sagernet.R
import io.nekohasekai.sagernet.Key
import io.nekohasekai.sagernet.SagerNet
import io.nekohasekai.sagernet.TunImplementation
import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.utils.DiagnosticLog
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import java.io.ByteArrayOutputStream
import java.net.HttpURLConnection
import java.net.InetSocketAddress
import java.net.Proxy
import java.net.URL

internal object ServiceAvailabilityProbe {
    private const val CONNECT_TIMEOUT_MS = 4_000
    private const val READ_TIMEOUT_MS = 4_000
    private const val MAX_GEMINI_BODY_BYTES = 64 * 1024

    enum class State {
        NOT_CHECKED,
        CHECKING,
        AVAILABLE,
        PAGE_REACHABLE,
        UNAVAILABLE,
        NO_RESPONSE,
        RESTRICTED,
        UNKNOWN,
        NO_TUNNEL_ROUTE,
        FORBIDDEN,
        CHALLENGE,
    }

    enum class Service(val nameRes: Int, val url: String) {
        GOOGLE(R.string.auto_vpn_service_google, "https://www.google.com/generate_204"),
        CHATGPT(R.string.auto_vpn_service_chatgpt, "https://chatgpt.com/"),
        GEMINI(R.string.auto_vpn_service_gemini, "https://gemini.google.com/"),
        TELEGRAM(R.string.auto_vpn_service_telegram, "https://web.telegram.org/"),
        YOUTUBE(R.string.auto_vpn_service_youtube, "https://www.youtube.com/generate_204"),
        INSTAGRAM(R.string.auto_vpn_service_instagram, "https://www.instagram.com/"),
    }

    data class Result(
        val service: Service,
        val state: State,
        val responseCode: Int?,
        val elapsedMillis: Long,
        val finalLocation: String? = null,
    )

    internal data class GeminiClassification(val state: State, val reason: String)

    fun checkingResults(): List<Result> = Service.values().map {
        Result(it, State.CHECKING, null, 0L)
    }

    fun notCheckedResults(): List<Result> = Service.values().map {
        Result(it, State.NOT_CHECKED, null, 0L)
    }

    suspend fun checkAll(renderGeminiPage: Boolean = false): List<Result> = coroutineScope {
        val renderedGeminiRouteAvailable = renderGeminiPage && supportsRenderedGeminiRoute()
        val proxy = activeProfileProxy()
        if (proxy == null && !renderedGeminiRouteAvailable) {
            if (renderGeminiPage) {
                DiagnosticLog.event(
                    "auto-vpn-services",
                    "gemini-probe-select path=none renderRequested=true reason=${renderedGeminiRouteUnavailableReason() ?: "no-active-vpn-route"}",
                )
            }
            return@coroutineScope Service.values().map { Result(it, State.NO_TUNNEL_ROUTE, null, 0L) }
        }
        val semaphore = Semaphore(3)
        Service.values().map { service ->
            async(Dispatchers.IO) {
                semaphore.withPermit {
                    val useRenderedGemini = service == Service.GEMINI && renderedGeminiRouteAvailable
                    val first = if (useRenderedGemini) {
                        checkGemini(renderPage = true)
                    } else {
                        if (service == Service.GEMINI) {
                            val reason = if (!renderGeminiPage) {
                                "render-not-requested"
                            } else {
                                renderedGeminiRouteUnavailableReason() ?: "rendered-probe-disabled"
                            }
                            DiagnosticLog.event(
                                "auto-vpn-services",
                                "gemini-probe-select path=http renderRequested=$renderGeminiPage reason=$reason",
                            )
                        }
                        if (proxy == null) {
                            Result(service, State.NO_TUNNEL_ROUTE, null, 0L)
                        } else {
                            checkOnce(service, proxy)
                        }
                    }
                    first
                }
            }
        }.awaitAll()
    }

    internal fun activeProfileProxy(): Proxy? {
        if (DataStore.serviceMode != Key.MODE_VPN) return null
        if (DataStore.requireSocks) {
            if (DataStore.socksUsername.isNotEmpty() || DataStore.socksPassword.isNotEmpty()) return null
            return Proxy(Proxy.Type.SOCKS, InetSocketAddress("127.0.0.1", DataStore.socksPort))
        }

        val selfExcludedFromAppVpn = DataStore.proxyApps && DataStore.bypass &&
                DataStore.individual.split('\n').any { it.trim() == SagerNet.application.packageName }
        return if (DataStore.tunImplementation == TunImplementation.SYSTEM && !selfExcludedFromAppVpn) {
            Proxy.NO_PROXY
        } else {
            null
        }
    }

    suspend fun checkGoogle(): Result {
        val proxy = activeProfileProxy()
            ?: return Result(Service.GOOGLE, State.NO_TUNNEL_ROUTE, null, 0L)
        return checkOnce(Service.GOOGLE, proxy)
    }

    suspend fun checkService(service: Service): Result {
        val proxy = activeProfileProxy()
            ?: return Result(service, State.NO_TUNNEL_ROUTE, null, 0L)
        return checkOnce(service, proxy)
    }

    suspend fun checkGemini(renderPage: Boolean = false): Result {
        if (renderPage && supportsRenderedGeminiRoute()) {
            DiagnosticLog.event(
                "auto-vpn-services",
                "gemini-probe-select path=rendered-webview renderRequested=true reason=active-system-vpn-route",
            )
            return GeminiRenderedPageProbe.check()
        }
        val proxy = activeProfileProxy()
            ?: run {
                DiagnosticLog.event(
                    "auto-vpn-services",
                    "gemini-probe-select path=none renderRequested=$renderPage reason=${renderedGeminiRouteUnavailableReason() ?: "no-active-vpn-route"}",
                )
                return Result(Service.GEMINI, State.NO_TUNNEL_ROUTE, null, 0L)
            }
        val reason = if (!renderPage) "render-not-requested" else renderedGeminiRouteUnavailableReason()
            ?: "rendered-probe-disabled"
        DiagnosticLog.event(
            "auto-vpn-services",
            "gemini-probe-select path=http renderRequested=$renderPage reason=$reason",
        )
        return checkOnce(Service.GEMINI, proxy)
    }

    private fun supportsRenderedGeminiRoute(): Boolean = renderedGeminiRouteUnavailableReason() == null

    private fun renderedGeminiRouteUnavailableReason(): String? {
        if (DataStore.serviceMode != Key.MODE_VPN) return "service-mode-not-vpn"
        if (DataStore.tunImplementation != TunImplementation.SYSTEM) return "system-tun-not-active"
        val selfExcludedFromAppVpn = DataStore.proxyApps && DataStore.bypass &&
                DataStore.individual.split('\n').any { it.trim() == SagerNet.application.packageName }
        if (selfExcludedFromAppVpn) return "app-excluded-from-vpn"
        return null
    }

    private suspend fun checkOnce(service: Service, proxy: Proxy): Result = withContext(Dispatchers.IO) {
        val startedAt = System.nanoTime()
        var connection: HttpURLConnection? = null
        try {
            connection = URL(service.url).openConnection(proxy) as HttpURLConnection
            connection.instanceFollowRedirects = true
            connection.connectTimeout = CONNECT_TIMEOUT_MS
            connection.readTimeout = READ_TIMEOUT_MS
            connection.requestMethod = "GET"
            connection.useCaches = false
            connection.setRequestProperty("Cache-Control", "no-cache, no-store")
            connection.setRequestProperty("Accept", "text/html,application/xhtml+xml,*/*")
            if (service == Service.GEMINI) connection.setRequestProperty("Accept-Encoding", "identity")

            val code = connection.responseCode
            val elapsed = elapsedMillis(startedAt)
            val finalLocation = safeLocation(connection.url)
            if (service == Service.GEMINI) {
                val body = readResponseBody(connection, code)
                val classification = classifyGeminiResponseDetailed(connection.url, code, body)
                DiagnosticLog.event(
                    "auto-vpn-services",
                    "gemini-probe-result path=http http=$code finalUrl=$finalLocation " +
                            "state=${classification.state.name} reason=${classification.reason} elapsedMs=$elapsed",
                )
                Result(service, classification.state, code, elapsed, finalLocation)
            } else {
                val state = when {
                    service == Service.GOOGLE && code == HttpURLConnection.HTTP_NO_CONTENT -> State.AVAILABLE
                    service == Service.GOOGLE -> State.UNAVAILABLE
                    code in 200..399 -> State.AVAILABLE
                    else -> State.UNKNOWN
                }
                Result(service, state, code, elapsed, finalLocation)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            if (service == Service.GEMINI) {
                DiagnosticLog.event(
                    "auto-vpn-services",
                    "gemini-probe-result path=http http=none finalUrl=unknown state=NO_RESPONSE " +
                            "reason=exception-${e.javaClass.simpleName} elapsedMs=${elapsedMillis(startedAt)}",
                )
            }
            Result(
                service,
                State.NO_RESPONSE,
                null,
                elapsedMillis(startedAt),
            )
        } finally {
            connection?.disconnect()
        }
    }

    internal fun classifyGeminiResponse(url: URL, code: Int, body: String): State {
        return classifyGeminiResponseDetailed(url, code, body).state
    }

    internal fun classifyGeminiResponseDetailed(url: URL, code: Int, body: String): GeminiClassification {
        val normalized = visibleHtmlText(body).replace(WHITESPACE, " ").lowercase()
        return when {
            isGoogleSorry(url) -> GeminiClassification(State.CHALLENGE, "google-sorry-url")
            code == 429 -> GeminiClassification(State.CHALLENGE, "http-429")
            hasGoogleChallenge(normalized) -> GeminiClassification(State.CHALLENGE, "google-challenge-marker")
            hasRegionalRestriction(normalized) -> GeminiClassification(State.RESTRICTED, "regional-restriction-marker")
            code == HttpURLConnection.HTTP_FORBIDDEN -> GeminiClassification(State.FORBIDDEN, "http-403")
            code >= 500 -> GeminiClassification(State.UNAVAILABLE, "server-error")
            code in 400..499 -> GeminiClassification(State.UNAVAILABLE, "http-client-error")
            url.host.equals("gemini.google.com", ignoreCase = true) && code in 200..399 ->
                GeminiClassification(State.PAGE_REACHABLE, "gemini-page-reachable-unverified")
            url.host.equals("accounts.google.com", ignoreCase = true) && code in 200..399 ->
                GeminiClassification(State.PAGE_REACHABLE, "google-login-page-unverified")
            else -> GeminiClassification(State.UNKNOWN, "no-known-gemini-marker")
        }
    }

    private fun visibleHtmlText(body: String): String = body
        .replace(SCRIPT_BLOCK, " ")
        .replace(STYLE_BLOCK, " ")
        .replace(HTML_COMMENT, " ")
        .replace(HTML_TAG, " ")
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")

    private fun readResponseBody(connection: HttpURLConnection, code: Int): String = runCatching {
        val stream = if (code >= HttpURLConnection.HTTP_BAD_REQUEST) {
            connection.errorStream
        } else {
            connection.inputStream
        } ?: return@runCatching ""
        stream.use { input ->
            val buffer = ByteArray(8 * 1024)
            val output = ByteArrayOutputStream()
            var remaining = MAX_GEMINI_BODY_BYTES
            while (remaining > 0) {
                val count = input.read(buffer, 0, minOf(buffer.size, remaining))
                if (count <= 0) break
                output.write(buffer, 0, count)
                remaining -= count
            }
            output.toString(Charsets.UTF_8.name()).lowercase()
        }
    }.getOrDefault("")

    private fun isGoogleSorry(url: URL): Boolean =
        url.host.equals("www.google.com", ignoreCase = true) && url.path.startsWith("/sorry/")

    private fun safeLocation(url: URL): String = "${url.protocol}://${url.host}${url.path}"

    private fun hasGoogleChallenge(body: String): Boolean {
        val normalized = body.lowercase()
        return listOf(
            "unusual traffic",
            "automated queries",
            "our systems have detected",
            "not a robot",
            "мы зарегистрировали подозрительный трафик",
            "подозрительный трафик, исходящий из вашей сети",
            "автоматические запросы",
            "подтвердите, что вы не робот",
        ).any { marker -> normalized.contains(marker) }
    }

    private fun hasRegionalRestriction(body: String): Boolean {
        val normalized = body.lowercase()

        return listOf(
            "not supported in your country",
            "not currently supported in your country",
            "not available in your country",
            "not yet available in your country",
            "isn't supported in your country",
            "isn't currently supported in your country",
            "isn't available in your country",
            "not supported in your region",
            "not available in your region",
            "isn't supported in your region",
            "isn't available in your region",
            "не поддерживается в вашей стране",
            "недоступен в вашей стране",
            "недоступна в вашей стране",
        ).any { marker -> normalized.contains(marker) }
    }

    private fun elapsedMillis(startedAtNanos: Long): Long =
        ((System.nanoTime() - startedAtNanos) / 1_000_000L).coerceAtLeast(0L)

    private val SCRIPT_BLOCK = Regex("""(?is)<script\b[^>]*>.*?</script\s*>""")
    private val STYLE_BLOCK = Regex("""(?is)<style\b[^>]*>.*?</style\s*>""")
    private val HTML_COMMENT = Regex("""(?s)<!--.*?-->""")
    private val HTML_TAG = Regex("""(?s)<[^>]*>""")
    private val WHITESPACE = Regex("\\s+")
}
