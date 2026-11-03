package io.nekohasekai.sagernet.bg

import android.Manifest
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.NetworkType
import androidx.work.PeriodicWorkRequest
import androidx.work.WorkerParameters
import androidx.work.multiprocess.RemoteWorkManager
import io.nekohasekai.sagernet.BuildConfig
import io.nekohasekai.sagernet.Key
import io.nekohasekai.sagernet.R
import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.ktx.Logs
import io.nekohasekai.sagernet.ktx.app
import io.nekohasekai.sagernet.ui.MainActivity
import io.nekohasekai.sagernet.ui.ReleaseUpdateChecker
import kotlinx.coroutines.CancellationException
import java.util.concurrent.TimeUnit

object ReleaseUpdateNotifier {
    const val CHANNEL_ID = "app-updates"
    const val ACTION_OPEN_UPDATE = "io.nekohasekai.sagernet.action.OPEN_UPDATE"

    private const val WORK_NAME = "ReleaseUpdateNotifications"
    private const val NOTIFICATION_ID = 48192
    private const val INTERVAL_HOURS = 12L

    suspend fun reconfigure() {
        val preferences = DataStore.configurationStore
        val enabled = (preferences.getBoolean(Key.CHECK_RELEASE_UPDATES) ?: true) ||
                (preferences.getBoolean(Key.CHECK_PRERELEASE_UPDATES) ?: false)
        val workManager = RemoteWorkManager.getInstance(app)
        if (!enabled) {
            workManager.cancelUniqueWork(WORK_NAME)
            return
        }

        val request = PeriodicWorkRequest.Builder(
            ReleaseUpdateNotificationWorker::class.java,
            INTERVAL_HOURS,
            TimeUnit.HOURS,
        )
            .setInitialDelay(INTERVAL_HOURS, TimeUnit.HOURS)
            .setConstraints(
                Constraints.Builder()
                    .setRequiredNetworkType(NetworkType.CONNECTED)
                    .build()
            )
            .build()

        workManager.enqueueUniquePeriodicWork(
            WORK_NAME,
            ExistingPeriodicWorkPolicy.KEEP,
            request,
        )
    }

    class ReleaseUpdateNotificationWorker(
        appContext: Context,
        params: WorkerParameters,
    ) : CoroutineWorker(appContext, params) {

        override suspend fun doWork(): Result {
            return try {
                val update = ReleaseUpdateChecker.findAvailableUpdate() ?: return Result.success()
                val notifications = NotificationManagerCompat.from(applicationContext)
                if (!notifications.areNotificationsEnabled()) return Result.success()
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
                    ContextCompat.checkSelfPermission(
                        applicationContext,
                        Manifest.permission.POST_NOTIFICATIONS,
                    ) != PackageManager.PERMISSION_GRANTED
                ) return Result.success()

                val appName = if (BuildConfig.FLAVOR == "clone") "TestExclaveD" else "ExclaveD"
                val releaseType = applicationContext.getString(
                    if (update.prerelease) R.string.app_update_prerelease else R.string.app_update_stable
                )
                val openApp = Intent(applicationContext, MainActivity::class.java).apply {
                    action = ACTION_OPEN_UPDATE
                    flags = Intent.FLAG_ACTIVITY_NEW_TASK or
                            Intent.FLAG_ACTIVITY_CLEAR_TOP or
                            Intent.FLAG_ACTIVITY_SINGLE_TOP
                }
                val pendingIntent = PendingIntent.getActivity(
                    applicationContext,
                    NOTIFICATION_ID,
                    openApp,
                    PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
                )
                val body = applicationContext.getString(
                    R.string.app_update_notification_text,
                    update.version,
                    releaseType,
                )
                val notification = NotificationCompat.Builder(applicationContext, CHANNEL_ID)
                    .setSmallIcon(R.drawable.ic_service_active)
                    .setContentTitle(applicationContext.getString(R.string.app_update_notification_title, appName))
                    .setContentText(body)
                    .setStyle(NotificationCompat.BigTextStyle().bigText(body))
                    .setContentIntent(pendingIntent)
                    .setCategory(NotificationCompat.CATEGORY_STATUS)
                    .setPriority(NotificationCompat.PRIORITY_DEFAULT)
                    .setAutoCancel(true)
                    .setOnlyAlertOnce(false)
                    .setWhen(System.currentTimeMillis())
                    .build()

                notifications.notify(NOTIFICATION_ID, notification)
                Result.success()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                Logs.w(e)
                Result.retry()
            }
        }
    }
}
