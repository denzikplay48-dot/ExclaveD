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
    }

    enum class Service(val nameRes: Int, val url: String) {
        CHATGPT(R.string.auto_vpn_service_chatgpt, "https://chatgpt.com/"),
        GEMINI(R.string.auto_vpn_service_gemini, "https://gemini.google.com/"),
        TELEGRAM(R.string.auto_vpn_service_telegram, "https://web.telegram.org/"),
        YOUTUBE(R.string.auto_vpn_service_youtube, "https://www.youtube.com/generate_204"),
        INSTAGRAM(R.string.auto_vpn_service_instagram, "https://www.instagram.com/"),
        GOOGLE(R.string.auto_vpn_service_google, "https://www.google.com/generate_204"),
    }

    data class Result(
        val service: Service,
        val state: State,
        val responseCode: Int?,
        val elapsedMillis: Long,
    )

    fun checkingResults(): List<Result> = Service.values().map {
        Result(it, State.CHECKING, null, 0L)
    }

    fun notCheckedResults(): List<Result> = Service.values().map {
        Result(it, State.NOT_CHECKED, null, 0L)
    }

    suspend fun checkAll(): List<Result> = coroutineScope {
        val proxy = activeProfileProxy()
            ?: return@coroutineScope Service.values().map { Result(it, State.NO_TUNNEL_ROUTE, null, 0L) }
        val semaphore = Semaphore(3)
        Service.values().map { service ->
            async(Dispatchers.IO) {
                semaphore.withPermit {
                    val first = checkOnce(service, proxy)
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
            if (service == Service.GEMINI && code in 200..399 && hasRegionalRestriction(connection)) {
                Result(service, State.RESTRICTED, code, elapsed)
            } else {
                val state = when {
                    service == Service.GOOGLE && code == HttpURLConnection.HTTP_NO_CONTENT -> State.AVAILABLE
                    service == Service.GOOGLE -> State.UNAVAILABLE
                    code in 200..399 -> State.AVAILABLE
                    else -> State.UNKNOWN
                }
                Result(service, state, code, elapsed)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (_: Exception) {
            Result(service, State.NO_RESPONSE, null, elapsedMillis(startedAt))
        } finally {
            connection?.disconnect()
        }
    }

    private fun hasRegionalRestriction(connection: HttpURLConnection): Boolean {
        val body = runCatching {
            val stream = connection.inputStream
            stream.use { input ->
                val buffer = ByteArray(8 * 1024)
                val output = StringBuilder()
                var remaining = MAX_GEMINI_BODY_BYTES
                while (remaining > 0) {
                    val count = input.read(buffer, 0, minOf(buffer.size, remaining))
                    if (count <= 0) break
                    output.append(String(buffer, 0, count, Charsets.UTF_8))
                    remaining -= count
                }
                output.toString()
            }
        }.getOrDefault("").lowercase()

        return listOf(
            "not supported in your country",
            "not currently supported in your country",
            "not available in your country",
            "not yet available in your country",
            "isn't currently supported in your country",
            "isn't available in your country",
            "не поддерживается в вашей стране",
            "недоступен в вашей стране",
            "недоступна в вашей стране",
        ).any { marker -> body.contains(marker) }
    }

    private fun elapsedMillis(startedAtNanos: Long): Long =
        ((System.nanoTime() - startedAtNanos) / 1_000_000L).coerceAtLeast(0L)
}
