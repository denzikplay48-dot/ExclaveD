/******************************************************************************
 *                                                                            *
 * Copyright (C) 2021 by nekohasekai <contact-sagernet@sekai.icu>             *
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

package io.nekohasekai.sagernet.bg.test

import android.net.Network
import android.net.NetworkCapabilities
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.nekohasekai.sagernet.Key
import io.nekohasekai.sagernet.SagerNet
import io.nekohasekai.sagernet.TunImplementation
import io.nekohasekai.sagernet.bg.GuardedProcessPool
import io.nekohasekai.sagernet.bg.ActiveVpnTrafficProbe
import io.nekohasekai.sagernet.bg.ExitIpGeoChecker
import io.nekohasekai.sagernet.bg.LocalResolver
import io.nekohasekai.sagernet.bg.proto.V2RayInstance
import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.database.ProxyEntity
import io.nekohasekai.sagernet.fmt.buildV2RayConfig
import io.nekohasekai.sagernet.ktx.Logs
import io.nekohasekai.sagernet.ktx.mkPort
import io.nekohasekai.sagernet.utils.DefaultNetworkListener
import io.nekohasekai.sagernet.utils.DiagnosticLog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import libsagernetcore.Libsagernetcore
import java.net.InetSocketAddress
import java.net.Proxy
import java.io.IOException
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

internal object TestProtectDiagnostics {
    private const val SUMMARY_INTERVAL_MS = 30_000L

    data class Snapshot(val calls: Long, val failures: Long)

    @Volatile
    private var activeInstances = 0
    private val calls = AtomicLong(0L)
    private val failures = AtomicLong(0L)
    private var lastSummaryAt = 0L

    @Synchronized
    fun begin() {
        activeInstances++
    }

    fun record(success: Boolean) {
        if (activeInstances == 0) return
        calls.incrementAndGet()
        if (!success) failures.incrementAndGet()
    }

    @Synchronized
    fun finish(): Snapshot? {
        if (activeInstances == 0) return null
        activeInstances--
        if (activeInstances != 0) return null

        val now = System.currentTimeMillis()
        if (lastSummaryAt != 0L && now - lastSummaryAt < SUMMARY_INTERVAL_MS) return null
        lastSummaryAt = now
        return Snapshot(calls.get(), failures.get()).also {
            calls.set(0L)
            failures.set(0L)
        }
    }
}

class V2RayTestInstance(
    profile: ProxyEntity,
    val link: String,
    val timeout: Int,
    val protectPath: String = "",
    private val enableHttpProxy: Boolean = false,
) : V2RayInstance(
    profile,
), LocalResolver {
    companion object {
        private const val ROUTE_DIAGNOSTIC_INTERVAL_MS = 30_000L
        private const val DIRECT_NETWORK_WAIT_TIMEOUT_MS = 5_000L
        private const val DIRECT_NETWORK_POLL_INTERVAL_MS = 100L
        private val lastRouteDiagnosticAt = AtomicLong(0L)
    }

    private val closeStarted = AtomicBoolean(false)
    private val protectMonitoringStarted = AtomicBoolean(false)
    private var httpProxyPort: Int? = null

    suspend fun doTest(): Int = withContext(Dispatchers.IO) {
        startCore()
        Libsagernetcore.urlTest(v2rayPoint, "", link, timeout)
    }

    suspend fun doTestHttp204(testUrl: String, timeoutMillis: Int): Int = withContext(Dispatchers.IO) {
        startCore()
        val port = httpProxyPort ?: throw IOException("Candidate SOCKS route is unavailable")
        ActiveVpnTrafficProbe.requireHttp204(
            testUrl,
            Proxy(Proxy.Type.SOCKS, InetSocketAddress("127.0.0.1", port)),
            timeoutMillis,
        )
    }

    suspend fun startForExitIpCheck() = withContext(Dispatchers.IO) {
        startCore()
    }

    suspend fun exitIpGeoInfo(): ExitIpGeoChecker.GeoInfo? {
        val port = httpProxyPort ?: return null
        val proxy = Proxy(Proxy.Type.SOCKS, InetSocketAddress("127.0.0.1", port))
        return ExitIpGeoChecker.geoInfo(proxy)
    }

    suspend fun exitIpCountryCode(): String? {
        return exitIpGeoInfo()?.countryCode
    }

    private suspend fun startCore() {
        processes = GuardedProcessPool {
            Logs.w(it)
        }
        init()
        ensureDirectTestNetwork()
        launch()
        if (pluginConfigs.isNotEmpty()) {
            delay(500L)
        }
    }

    @Volatile
    override var underlyingNetwork: Network? = null

    override suspend fun init() {
        super.init()
        if (protectMonitoringStarted.compareAndSet(false, true)) {
            TestProtectDiagnostics.begin()
        }
        v2rayPoint.withProtect(protectPath)
        v2rayPoint.withLocalResolver(this)
        DefaultNetworkListener.start(this) {
            underlyingNetwork = it
            if (it != null) logRouteDiagnostics(it)
        }
    }

    private suspend fun ensureDirectTestNetwork() {
        val physicalNetwork = withTimeoutOrNull(DIRECT_NETWORK_WAIT_TIMEOUT_MS) {
            var readyNetwork: Network? = null
            while (readyNetwork == null) {
                val network = underlyingNetwork
                val capabilities = network?.let {
                    runCatching { SagerNet.connectivity.getNetworkCapabilities(it) }.getOrNull()
                }
                if (network != null && capabilities?.hasCapability(
                        NetworkCapabilities.NET_CAPABILITY_NOT_VPN
                    ) == true
                ) {
                    readyNetwork = network
                } else {
                    delay(DIRECT_NETWORK_POLL_INTERVAL_MS)
                }
            }
            readyNetwork
        } ?: run {
            DiagnosticLog.error(
                "profile-test-route",
                "test-route-guard result=blocked reason=no-non-vpn-network"
            )
            throw CancellationException("No non-VPN network; refusing to test the profile through VPN")
        }

        val systemVpnActive = DataStore.serviceMode == Key.MODE_VPN &&
                DataStore.tunImplementation == TunImplementation.SYSTEM &&
                (SagerNet.started || DataStore.startedProfile > 0L)
        val protectSocketReady = if (systemVpnActive && protectPath.isNotBlank()) {
            withTimeoutOrNull(DIRECT_NETWORK_WAIT_TIMEOUT_MS) {
                while (!File(protectPath).exists()) delay(DIRECT_NETWORK_POLL_INTERVAL_MS)
                true
            } == true
        } else {
            false
        }
        if (systemVpnActive && (protectPath.isBlank() || !protectSocketReady)) {
            DiagnosticLog.error(
                "profile-test-route",
                "test-route-guard result=blocked reason=protect-socket-unavailable " +
                        "protectPathConfigured=${protectPath.isNotBlank()} " +
                        "protectSocketExists=$protectSocketReady"
            )
            throw CancellationException("VPN bypass unavailable: the socket-protection path is missing")
        }

        val bypassMode = when {
            systemVpnActive -> "vpn-protect-socket"
            DataStore.serviceMode == Key.MODE_VPN && DataStore.tunImplementation != TunImplementation.SYSTEM &&
                    (SagerNet.started || DataStore.startedProfile > 0L) -> "app-excluded-from-vpn"
            else -> "physical-network"
        }
        DiagnosticLog.event(
            "profile-test-route",
            "test-route-guard result=ready bypassMode=$bypassMode " +
                    "resolverNetwork=non-vpn protectPathConfigured=${protectPath.isNotBlank()} " +
                    "protectSocketExists=${protectPath.isNotBlank() && File(protectPath).exists()}"
        )
        logRouteDiagnostics(physicalNetwork)
    }

    private fun logRouteDiagnostics(network: Network) {
        val now = System.currentTimeMillis()
        while (true) {
            val previous = lastRouteDiagnosticAt.get()
            if (now - previous < ROUTE_DIAGNOSTIC_INTERVAL_MS) return
            if (lastRouteDiagnosticAt.compareAndSet(previous, now)) break
        }

        val capabilities = runCatching {
            SagerNet.connectivity.getNetworkCapabilities(network)
        }.getOrNull()
        val route = when {
            capabilities == null -> "unknown"
            capabilities.hasTransport(NetworkCapabilities.TRANSPORT_VPN) -> "vpn"
            capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_NOT_VPN) -> "non-vpn"
            else -> "other"
        }
        val socketExists = protectPath.isNotEmpty() && File(protectPath).exists()
        DiagnosticLog.event(
            "profile-test-route",
            "test-route-state protectPathConfigured=${protectPath.isNotEmpty()} protectSocketExists=$socketExists " +
                    "resolverNetwork=$route wifi=${capabilities?.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)} " +
                    "cellular=${capabilities?.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR)}"
        )
    }

    override fun close() {
        if (!closeStarted.compareAndSet(false, true)) return
        runCatching {
            v2rayPoint.withLocalResolver(null)
        }.onFailure {
            if (it !is UninitializedPropertyAccessException) {
                Logs.w(it)
            }
        }
        runCatching {
            runBlocking {
                processes.closeAndJoin()
            }
        }.onFailure {
            if (it !is UninitializedPropertyAccessException) {
                Logs.w(it)
            }
        }
        runCatching {
            runBlocking {
                DefaultNetworkListener.stop(this@V2RayTestInstance)
            }
        }.onFailure {
            Logs.w(it)
        }
        try {
            super.close()
        } finally {
            if (protectMonitoringStarted.compareAndSet(true, false)) {
                TestProtectDiagnostics.finish()?.let { snapshot ->
                    DiagnosticLog.event(
                        "profile-test-route",
                        "vpn-protect-window calls=${snapshot.calls} failures=${snapshot.failures}"
                    )
                }
            }
        }
    }

    override fun buildConfig() {
        config = buildV2RayConfig(profile, forTest = true)
        if (!enableHttpProxy) return

        val port = mkPort()
        val root = JsonParser.parseString(config.config).asJsonObject
        val inbounds = root.getAsJsonArray("inbounds") ?: JsonArray().also { root.add("inbounds", it) }
        inbounds.add(JsonObject().apply {
            addProperty("tag", "exclave-exit-geoip")
            addProperty("listen", "127.0.0.1")
            addProperty("port", port)
            addProperty("protocol", "socks")
            add("settings", JsonObject().apply {
                addProperty("auth", "noauth")
                addProperty("udp", false)
            })
        })

        val outboundTag = config.outboundTagsCurrent.firstOrNull()
            ?: error("Candidate test outbound is unavailable")
        val routing = root.getAsJsonObject("routing") ?: JsonObject().also { root.add("routing", it) }
        val rules = routing.getAsJsonArray("rules") ?: JsonArray()
        val geoIpRoute = JsonObject().apply {
            addProperty("type", "field")
            add("inboundTag", JsonArray().apply { add("exclave-exit-geoip") })
            addProperty("outboundTag", outboundTag)
        }
        routing.add("rules", JsonArray().apply {
            add(geoIpRoute)
            rules.forEach { add(it) }
        })
        config.config = root.toString()
        httpProxyPort = port
    }
}
