package io.nekohasekai.sagernet.ui

import android.content.Context
import io.nekohasekai.sagernet.BuildConfig
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext

internal class AppUpdateDownloader(
    context: Context,
    private val update: AvailableAppUpdate,
) {
    private val appContext = context.applicationContext

    @Volatile
    private var activeConnection: HttpURLConnection? = null

    fun cancel() {
        activeConnection?.disconnect()
    }

    suspend fun download(onProgress: (downloaded: Long, total: Long) -> Unit): File =
        withContext(Dispatchers.IO) {
            val downloadUrl = URL(update.downloadUrl)
            if (downloadUrl.protocol != "https") throw IOException("Insecure update URL")

            val safeAssetName = update.assetName.takeIf {
                it.endsWith(".apk", ignoreCase = true) && it.matches(Regex("[A-Za-z0-9._-]+"))
            } ?: "${BuildConfig.APPLICATION_ID}-${update.version}.apk"
            val directory = File(appContext.cacheDir, "app-update")
            if (!directory.exists() && !directory.mkdirs()) throw IOException("Cannot create update cache")
            val target = File(directory, safeAssetName)
            val partial = File(directory, "$safeAssetName.part")
            partial.delete()

            val connection = (downloadUrl.openConnection() as HttpURLConnection).apply {
                connectTimeout = 20_000
                readTimeout = 60_000
                requestMethod = "GET"
                instanceFollowRedirects = true
                setRequestProperty("Accept", "application/vnd.android.package-archive")
                setRequestProperty("Accept-Encoding", "identity")
                setRequestProperty("User-Agent", "ExclaveD/${BuildConfig.VERSION_NAME}")
            }
            activeConnection = connection
            var completed = false
            try {
                connection.connect()
                if (connection.url.protocol != "https") throw IOException("Insecure update redirect")
                if (connection.responseCode != HttpURLConnection.HTTP_OK) {
                    throw IOException("Update download failed: HTTP ${connection.responseCode}")
                }

                val contentLength = connection.getHeaderField("Content-Length")
                    ?.toLongOrNull()
                    ?.takeIf { it > 0L }
                    ?: update.downloadSize
                var downloaded = 0L
                var lastProgressAt = 0L
                onProgress(0L, contentLength)
                BufferedInputStream(connection.inputStream).use { input ->
                    BufferedOutputStream(FileOutputStream(partial)).use { output ->
                        val buffer = ByteArray(128 * 1024)
                        while (true) {
                            currentCoroutineContext().ensureActive()
                            val count = input.read(buffer)
                            if (count < 0) break
                            output.write(buffer, 0, count)
                            downloaded += count
                            val now = System.currentTimeMillis()
                            if (now - lastProgressAt >= 200L) {
                                lastProgressAt = now
                                onProgress(downloaded, contentLength)
                            }
                        }
                    }
                }
                if (downloaded <= 0L) throw IOException("Downloaded update is empty")
                if (contentLength > 0L && downloaded != contentLength) {
                    throw IOException("Update download was incomplete")
                }
                if (update.downloadSize > 0L && downloaded != update.downloadSize) {
                    throw IOException("Downloaded update size does not match release metadata")
                }
                onProgress(downloaded, contentLength)

                if (target.exists() && !target.delete()) throw IOException("Cannot replace cached update")
                if (!partial.renameTo(target)) throw IOException("Cannot finalize downloaded update")
                completed = true
                target
            } finally {
                activeConnection = null
                connection.disconnect()
                if (!completed) partial.delete()
            }
        }
}
