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
    private const val RETRY_DELAY_MS = 1_000L

    enum class State {
        NOT_CHECKED,
        CHECKING,
        AVAILABLE,
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

    fun checkingResults(): List<Result> = Service.values().map {
        Result(it, State.CHECKING, null, 0L)
    }

    fun notCheckedResults(): List<Result> = Service.values().map {
        Result(it, State.NOT_CHECKED, null, 0L)
    }

    suspend fun checkAll(renderGeminiPage: Boolean = false): List<Result> = coroutineScope {
        val proxy = activeProfileProxy()
            ?: return@coroutineScope Service.values().map { Result(it, State.NO_TUNNEL_ROUTE, null, 0L) }
        val semaphore = Semaphore(3)
        Service.values().map { service ->
            async(Dispatchers.IO) {
                semaphore.withPermit {
                    val useRenderedGemini = service == Service.GEMINI && renderGeminiPage && supportsRenderedGeminiRoute()
                    val first = if (useRenderedGemini) {
                        checkGemini(renderPage = true)
                    } else {
                        checkOnce(service, proxy)
                    }
                    if (useRenderedGemini) {
                        return@withPermit first
                    }
                    if (first.state != State.NO_RESPONSE) return@withPermit first
                    delay(RETRY_DELAY_MS)
                    val second = checkOnce(service, proxy)
                    if (second.state == State.NO_RESPONSE) {
                        second.copy(elapsedMillis = first.elapsedMillis + RETRY_DELAY_MS + second.elapsedMillis)
                    } else {
                        second
                    }
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
        val first = checkOnce(service, proxy)
        if (first.state != State.NO_RESPONSE) return first
        delay(RETRY_DELAY_MS)
        val second = checkOnce(service, proxy)
        return if (second.state == State.NO_RESPONSE) {
            second.copy(elapsedMillis = first.elapsedMillis + RETRY_DELAY_MS + second.elapsedMillis)
        } else {
            second
        }
    }

    suspend fun checkGemini(renderPage: Boolean = false): Result {
        val proxy = activeProfileProxy()
            ?: return Result(Service.GEMINI, State.NO_TUNNEL_ROUTE, null, 0L)
        if (renderPage && supportsRenderedGeminiRoute()) return GeminiRenderedPageProbe.check()
        return checkOnce(Service.GEMINI, proxy)
    }

    private fun supportsRenderedGeminiRoute(): Boolean {
        if (DataStore.serviceMode != Key.MODE_VPN || DataStore.requireSocks ||
            DataStore.tunImplementation != TunImplementation.SYSTEM
        ) return false
        val selfExcludedFromAppVpn = DataStore.proxyApps && DataStore.bypass &&
                DataStore.individual.split('\n').any { it.trim() == SagerNet.application.packageName }
        return !selfExcludedFromAppVpn
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
                val state = classifyGeminiResponse(connection.url, code, body)
                Result(service, state, code, elapsed, finalLocation)
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
        } catch (_: Exception) {
            Result(service, State.NO_RESPONSE, null, elapsedMillis(startedAt))
        } finally {
            connection?.disconnect()
        }
    }

    internal fun classifyGeminiResponse(url: URL, code: Int, body: String): State {
        val normalized = body.lowercase()
        return when {
            isGoogleSorry(url) || hasGoogleChallenge(normalized) -> State.CHALLENGE
            hasRegionalRestriction(normalized) -> State.RESTRICTED
            code == HttpURLConnection.HTTP_FORBIDDEN -> State.FORBIDDEN
            url.host.equals("gemini.google.com", ignoreCase = true) &&
                    url.path.trimEnd('/').let { it == "/app" || it.startsWith("/app/") } &&
                    code in 200..399 -> State.AVAILABLE
            code >= 500 -> State.UNAVAILABLE
            else -> State.UNKNOWN
        }
    }

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
            "не поддерживается в вашей стране",
            "недоступен в вашей стране",
            "недоступна в вашей стране",
        ).any { marker -> normalized.contains(marker) }
    }

    private fun elapsedMillis(startedAtNanos: Long): Long =
        ((System.nanoTime() - startedAtNanos) / 1_000_000L).coerceAtLeast(0L)
}
