package com.example.haade_panel_s504

import android.util.Log
import org.eclipse.paho.client.mqttv3.IMqttActionListener
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.IMqttToken
import org.eclipse.paho.client.mqttv3.MqttAsyncClient
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLSocketFactory

/**
 * One MQTT connection that never gives up.
 *
 * The original app retried from a timer that its own connect() cancelled, so one failed attempt
 * (broker still restarting) ended reconnection for good. Here every failure path schedules the
 * next attempt (1, 2, 4 … 30 s), a supervisor re-checks every minute, and after *every* connect
 * the command topics are subscribed again before the panel reports "online".
 *
 * All connection bookkeeping runs on one control thread; [publish] may be called from any thread.
 */
class MqttLink(private val cfg: Config, private val listener: Listener) {

    data class Config(
        val host: String,
        val port: Int,
        val user: String,
        val pass: String,
        val tls: Boolean,
        val clientId: String,
        val topics: Topics,
    ) {
        val uri: String get() = (if (tls) "ssl://" else "tcp://") + host.trim() + ":" + port
    }

    enum class State { CONNECTING, CONNECTED, RETRY }

    interface Listener {
        /** Connected and subscribed: publish discovery, availability and current states now. */
        fun onReady()
        fun onMessage(topic: String, payload: String)
        fun onState(state: State, detail: String)
    }

    private val ctl: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "mqtt-ctl").apply { isDaemon = true }
    }

    private val subscriptions = arrayOf(cfg.topics.ledSet, cfg.topics.relaySet(1), cfg.topics.relaySet(2), Topics.HA_STATUS)

    @Volatile private var client: MqttAsyncClient? = null
    @Volatile private var stopped = false
    private var connecting = false
    private var retry: ScheduledFuture<*>? = null
    private var backoffSec = 1L

    fun start() {
        post { connectNow() }
        // Safety net: if nothing is connected, connecting or scheduled, try again.
        ctl.scheduleWithFixedDelay({
            try {
                if (!stopped && !connecting && retry == null && !isConnected()) connectNow()
            } catch (t: Throwable) {
                Log.e(TAG, "supervisor", t)
            }
        }, 60, 60, TimeUnit.SECONDS)
    }

    /** The network came back: retry now instead of waiting out the backoff. */
    fun kick() = post {
        if (!stopped && !connecting && !isConnected()) {
            retry?.cancel(false)
            retry = null
            backoffSec = 1
            connectNow()
        }
    }

    fun isConnected(): Boolean = client?.isConnected == true

    fun publish(topic: String, payload: String, retain: Boolean) {
        val c = client ?: return
        try {
            if (c.isConnected) c.publish(topic, payload.toByteArray(Charsets.UTF_8), 1, retain)
        } catch (e: Exception) {
            Log.w(TAG, "publish $topic: ${e.message}")
        }
    }

    /** Blocks up to a few seconds: optionally announces "offline", then closes the connection. */
    fun stop(publishOffline: Boolean) {
        stopped = true
        val done = try {
            ctl.submit(Runnable {
                retry?.cancel(false)
                retry = null
                client?.let { c ->
                    client = null
                    shutdown(c, publishOffline)
                }
            })
        } catch (e: RejectedExecutionException) {
            null
        }
        try {
            done?.get(6, TimeUnit.SECONDS)
        } catch (_: Exception) {
        }
        ctl.shutdownNow()
    }

    private fun connectNow() {
        if (stopped || connecting || isConnected()) return
        retry = null
        val c = client ?: try {
            MqttAsyncClient(cfg.uri, cfg.clientId, MemoryPersistence()).also { created ->
                created.setCallback(callbackFor(created))
                client = created
            }
        } catch (e: Exception) {
            scheduleRetry(describe(e))
            return
        }
        val options = MqttConnectOptions().apply {
            isCleanSession = true
            keepAliveInterval = 30
            connectionTimeout = 10
            isAutomaticReconnect = false
            maxInflight = 64
            if (cfg.user.isNotEmpty()) userName = cfg.user
            if (cfg.pass.isNotEmpty()) password = cfg.pass.toCharArray()
            setWill(cfg.topics.avail, OFFLINE, 1, true)
            if (cfg.tls) socketFactory = SSLSocketFactory.getDefault()
        }
        connecting = true
        listener.onState(State.CONNECTING, "")
        try {
            c.connect(options, null, object : IMqttActionListener {
                override fun onSuccess(token: IMqttToken?) = post {
                    connecting = false
                    if (client === c) subscribeAll(c)
                }

                override fun onFailure(token: IMqttToken?, e: Throwable?) = post {
                    connecting = false
                    if (client === c) restart(c, describe(e))
                }
            })
        } catch (e: Exception) {
            connecting = false
            restart(c, describe(e))
        }
    }

    private fun subscribeAll(c: MqttAsyncClient) {
        if (stopped) return
        try {
            c.subscribe(subscriptions, IntArray(subscriptions.size) { 1 }, null, object : IMqttActionListener {
                override fun onSuccess(token: IMqttToken?) = post {
                    if (stopped || client !== c) return@post
                    if (token?.grantedQos?.any { it == 0x80 } == true) {
                        restart(c, "subscription rejected by broker")
                        return@post
                    }
                    backoffSec = 1
                    listener.onState(State.CONNECTED, "")
                    listener.onReady()
                }

                override fun onFailure(token: IMqttToken?, e: Throwable?) = post {
                    if (client === c) restart(c, "subscribe: " + describe(e))
                }
            })
        } catch (e: Exception) {
            restart(c, "subscribe: " + describe(e))
        }
    }

    private fun callbackFor(c: MqttAsyncClient) = object : MqttCallbackExtended {
        override fun connectComplete(reconnect: Boolean, serverURI: String?) {}

        override fun connectionLost(cause: Throwable?) = post {
            if (client === c) restart(c, describe(cause))
        }

        override fun messageArrived(topic: String, message: MqttMessage) {
            listener.onMessage(topic, String(message.payload, Charsets.UTF_8))
        }

        override fun deliveryComplete(token: IMqttDeliveryToken?) {}
    }

    /** Drops the client completely (fresh threads and state next time) and schedules a retry. */
    private fun restart(c: MqttAsyncClient, reason: String) {
        if (client === c) client = null
        connecting = false
        shutdown(c, publishOffline = false)
        scheduleRetry(reason)
    }

    private fun scheduleRetry(reason: String) {
        if (stopped) return
        Log.i(TAG, "retry in ${backoffSec}s: $reason")
        listener.onState(State.RETRY, reason)
        retry?.cancel(false)
        val delay = backoffSec
        backoffSec = (backoffSec * 2).coerceAtMost(MAX_BACKOFF_SEC)
        retry = try {
            ctl.schedule(Runnable {
                retry = null
                try {
                    connectNow()
                } catch (t: Throwable) {
                    Log.e(TAG, "connect", t)
                }
            }, delay, TimeUnit.SECONDS)
        } catch (e: RejectedExecutionException) {
            null
        }
    }

    private fun shutdown(c: MqttAsyncClient, publishOffline: Boolean) {
        try {
            if (publishOffline && c.isConnected) c.publish(cfg.topics.avail, OFFLINE, 1, true).waitForCompletion(2000)
        } catch (_: Exception) {
        }
        try {
            if (c.isConnected) c.disconnect(0).waitForCompletion(2000)
        } catch (_: Exception) {
        }
        try {
            c.disconnectForcibly(0, 500)
        } catch (_: Exception) {
        }
        try {
            c.close()
        } catch (_: Exception) {
        }
    }

    private fun post(block: () -> Unit) {
        try {
            ctl.execute {
                try {
                    block()
                } catch (t: Throwable) {
                    Log.e(TAG, "mqtt-ctl", t)
                }
            }
        } catch (_: RejectedExecutionException) {
        }
    }

    companion object {
        private const val TAG = "PanelMqtt"
        private const val MAX_BACKOFF_SEC = 30L
        private val OFFLINE = "offline".toByteArray()

        fun describe(e: Throwable?): String = when (e) {
            null -> "connection lost"
            is MqttException -> when (e.reasonCode) {
                MqttException.REASON_CODE_FAILED_AUTHENTICATION.toInt(),
                MqttException.REASON_CODE_NOT_AUTHORIZED.toInt() -> "wrong login or password"
                MqttException.REASON_CODE_SERVER_CONNECT_ERROR.toInt() -> e.cause?.message ?: "broker unreachable"
                else -> e.cause?.message ?: e.message ?: "MQTT error ${e.reasonCode}"
            }
            else -> e.message ?: e.javaClass.simpleName
        }
    }
}
