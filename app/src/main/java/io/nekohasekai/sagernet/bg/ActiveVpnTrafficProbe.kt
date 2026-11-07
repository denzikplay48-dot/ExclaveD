package io.nekohasekai.sagernet.bg

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.net.HttpURLConnection
import java.net.Proxy
import java.net.URL
import java.io.IOException

internal object ActiveVpnTrafficProbe {
    const val CLOUDFLARE_204_URL = "http://cp.cloudflare.com/generate_204"
    private const val CLOUDFLARE_DOWNLOAD_URL = "https://speed.cloudflare.com/__down"
    private const val DOWNLOAD_BYTES = 256 * 1024
    private const val CONNECT_TIMEOUT_MS = 5_000
    private const val READ_TIMEOUT_MS = 4_000
    private const val MAX_DOWNLOAD_TIME_MS = 12_000L

    data class EndpointResult(
        val host: String,
        val responseCode: Int?,
        val elapsedMillis: Int,
        val success: Boolean,
        val error: String? = null,
    )

    data class DownloadResult(
        val downloadedBytes: Long,
        val expectedBytes: Long,
        val elapsedMillis: Long,
        val firstByteMillis: Int?,
        val bytesPerSecond: Long?,
        val complete: Boolean,
        val error: String? = null,
    )

    data class Result(
        val endpoints: List<EndpointResult>,
        val download: DownloadResult,
        val downloadConfirmed: Boolean? = null,
    ) {
        val hasTraffic: Boolean
            get() = endpoints.any { it.success } || download.downloadedBytes > 0L || downloadConfirmed == true

        val downloadUsable: Boolean?
            get() = if (download.expectedBytes == 0L) null else downloadConfirmed ?: download.complete

        val latencyMillis: Int?
            get() = endpoints.asSequence()
                .filter { it.success }
                .map { it.elapsedMillis }
                .minOrNull()
                ?: download.firstByteMillis
    }

    suspend fun run(
        connectionTestUrl: String,
        proxy: Proxy,
        downloadEnabled: Boolean = false,
    ): Result = withContext(Dispatchers.IO) {
        val urls = linkedMapOf<String, String>()
        listOf(connectionTestUrl, CLOUDFLARE_204_URL).forEach { value ->
            runCatching { URL(value.trim()) }.getOrNull()?.let { url ->
                val host = url.host.lowercase()
                if (host !in urls) urls[host] = url.toString()
            }
        }
        Result(
            endpoints = urls.values.map { checkEndpoint(it, proxy, CONNECT_TIMEOUT_MS) },
            download = if (downloadEnabled) checkDownload(proxy) else skippedDownload(),
        )
    }

    suspend fun repeatDownload(proxy: Proxy): DownloadResult = withContext(Dispatchers.IO) {
        checkDownload(proxy)
    }

    fun checkHttp204(value: String, proxy: Proxy, timeoutMillis: Int): EndpointResult =
        checkEndpoint(value, proxy, timeoutMillis.coerceAtLeast(1))

    fun requireHttp204(value: String, proxy: Proxy, timeoutMillis: Int): Int {
        val result = checkHttp204(value, proxy, timeoutMillis)
        if (!result.success) {
            throw IOException("Expected HTTP 204 from ${result.host}, got ${result.responseCode ?: result.error ?: "no response"}")
        }
        return result.elapsedMillis
    }

    private fun checkEndpoint(value: String, proxy: Proxy, timeoutMillis: Int): EndpointResult {
        val url = URL(value)
        val start = System.nanoTime()
        var connection: HttpURLConnection? = null
        return try {
            connection = url.openConnection(proxy) as? HttpURLConnection
                ?: return EndpointResult(url.host, null, 0, false, "unsupported-connection")
            connection.instanceFollowRedirects = false
            connection.connectTimeout = timeoutMillis
            connection.readTimeout = timeoutMillis
            connection.requestMethod = "GET"
            connection.useCaches = false
            connection.setRequestProperty("Cache-Control", "no-cache, no-store")
            connection.setRequestProperty("Pragma", "no-cache")
            val code = connection.responseCode
            val elapsed = elapsedMillis(start)
            val success = code == HttpURLConnection.HTTP_NO_CONTENT
            EndpointResult(url.host, code, elapsed, success)
        } catch (e: Exception) {
            EndpointResult(url.host, null, elapsedMillis(start), false, e.javaClass.simpleName)
        } finally {
            connection?.disconnect()
        }
    }

    private fun skippedDownload() = DownloadResult(
        downloadedBytes = 0L,
        expectedBytes = 0L,
        elapsedMillis = 0L,
        firstByteMillis = null,
        bytesPerSecond = null,
        complete = true,
        error = "disabled",
    )

    private fun checkDownload(proxy: Proxy): DownloadResult {
        val expected = DOWNLOAD_BYTES.toLong()
        val url = URL("$CLOUDFLARE_DOWNLOAD_URL?bytes=$DOWNLOAD_BYTES&probe=${System.nanoTime()}")
        val start = System.nanoTime()
        var firstByteMillis: Int? = null
        var downloaded = 0L
        var connection: HttpURLConnection? = null
        return try {
            connection = url.openConnection(proxy) as? HttpURLConnection
                ?: return DownloadResult(0L, expected, 0L, null, null, false, "unsupported-connection")
            connection.instanceFollowRedirects = true
            connection.connectTimeout = CONNECT_TIMEOUT_MS
            connection.readTimeout = READ_TIMEOUT_MS
            connection.requestMethod = "GET"
            connection.useCaches = false
            connection.setRequestProperty("Cache-Control", "no-cache, no-store")
            val code = connection.responseCode
            if (code !in 200..299) {
                return DownloadResult(
                    0L, expected, elapsedMillisLong(start), null, null, false, "http-$code",
                )
            }

            val buffer = ByteArray(16 * 1024)
            connection.inputStream.use { input ->
                while (downloaded < expected && elapsedMillisLong(start) < MAX_DOWNLOAD_TIME_MS) {
                    val count = input.read(buffer, 0, minOf(buffer.size.toLong(), expected - downloaded).toInt())
                    if (count < 0) break
                    if (count == 0) continue
                    if (firstByteMillis == null) firstByteMillis = elapsedMillis(start)
                    downloaded += count
                }
            }
            val elapsed = elapsedMillisLong(start)
            DownloadResult(
                downloadedBytes = downloaded,
                expectedBytes = expected,
                elapsedMillis = elapsed,
                firstByteMillis = firstByteMillis,
                bytesPerSecond = if (elapsed > 0L) downloaded * 1000L / elapsed else null,
                complete = downloaded >= expected,
            )
        } catch (e: Exception) {
            val elapsed = elapsedMillisLong(start)
            DownloadResult(
                downloadedBytes = downloaded,
                expectedBytes = expected,
                elapsedMillis = elapsed,
                firstByteMillis = firstByteMillis,
                bytesPerSecond = if (elapsed > 0L) downloaded * 1000L / elapsed else null,
                complete = downloaded >= expected,
                error = e.javaClass.simpleName,
            )
        } finally {
            connection?.disconnect()
        }
    }

    private fun elapsedMillis(startNanos: Long): Int =
        (elapsedMillisLong(startNanos)).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()

    private fun elapsedMillisLong(startNanos: Long): Long =
        ((System.nanoTime() - startNanos) / 1_000_000L).coerceAtLeast(0L)
}
