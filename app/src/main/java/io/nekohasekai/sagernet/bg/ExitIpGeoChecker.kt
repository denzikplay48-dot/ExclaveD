package io.nekohasekai.sagernet.bg

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.Proxy
import java.net.URL
import java.util.Locale

object ExitIpGeoChecker {
    private const val GEOIP_URL = "https://api.ip.sb/geoip"
    private const val TIMEOUT_MILLIS = 4_000

    data class GeoInfo(val countryCode: String?, val countryName: String?)

    suspend fun countryCode(proxy: Proxy? = null): String? = geoInfo(proxy)?.countryCode

    suspend fun geoInfo(proxy: Proxy? = null): GeoInfo? = withContext(Dispatchers.IO) {
        var connection: HttpURLConnection? = null
        try {
            val openedConnection = (if (proxy == null) {
                URL(GEOIP_URL).openConnection()
            } else {
                URL(GEOIP_URL).openConnection(proxy)
            }) as? HttpURLConnection ?: return@withContext null
            connection = openedConnection
            openedConnection.instanceFollowRedirects = true
            openedConnection.connectTimeout = TIMEOUT_MILLIS
            openedConnection.readTimeout = TIMEOUT_MILLIS
            openedConnection.setRequestProperty("User-Agent", "ExclaveD/exit-ip-check")
            if (openedConnection.responseCode !in 200..299) return@withContext null

            val body = openedConnection.inputStream.bufferedReader(Charsets.UTF_8).use { it.readText() }
            val response = JSONObject(body)
            if (response.optString("ip").trim().isBlank()) return@withContext null
            val code = response.optString("country_code").trim().uppercase(Locale.ROOT)
            val countryCode = if (code in Locale.getISOCountries()) {
                code
            } else if (response.optString("country").trim().equals("Russia", ignoreCase = true) ||
                response.optString("country").trim().equals("Russian Federation", ignoreCase = true)
            ) {
                "RU"
            } else {
                null
            }
            val countryName = countryCode?.let {
                Locale("", it).getDisplayCountry(Locale.getDefault()).takeIf(String::isNotBlank)
            } ?: response.optString("country").trim().takeIf(String::isNotBlank)
            GeoInfo(countryCode, countryName)
        } catch (e: CancellationException) {
            throw e
        } catch (_: Exception) {
            null
        } finally {
            connection?.disconnect()
        }
    }
}
