package no.ks.kes.grpc

import com.eventstore.dbclient.*
import mu.KotlinLogging
import no.ks.kes.lib.*
import no.ks.kes.lib.EventData
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ExecutionException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

class GrpcEventSubscriberFactory(
    private val eventStoreDBClient: EventStoreDBClient,
    private val serdes: EventSerdes,
    private val category: String,
    private val metadataSerdes: EventMetadataSerdes<out Metadata>? = null,
) : EventSubscriberFactory<GrpcSubscriptionWrapper> {

    override fun getSerializationId(eventDataClass: KClass<EventData<*>>): String =
        serdes.getSerializationId(eventDataClass)

    override fun createSubscriber(
        hwmId: String,
        fromEvent: Long,
        onEvent: (EventWrapper<EventData<*>>) -> Unit,
        onError: (Exception) -> Unit,
        onLive: () -> Unit
    ): GrpcSubscriptionWrapper {
        return GrpcSubscriptionWrapper(eventStoreDBClient, category, hwmId, fromEvent, serdes, metadataSerdes, onEvent, onError, onLive)
    }

}

class SubscriptionLiveCheckpoint(private val eventStoreDBClient: EventStoreDBClient, private val streamId: String) {

    private val lock = ReentrantLock()

    private val liveCheckpointTimeout = Duration.ofSeconds(10)

    private var timestamp: Instant = Instant.now()
    private var lastEvent: Long = eventStoreDBClient.getSubscriptionLiveCheckpoint(streamId).also {
        log.debug { "Setting live checkpoint for $streamId at $it" }
    }
    private var isLive: Boolean = false

    fun isLive() = isLive

    fun triggerOnceIfSubscriptionIsLive(eventNumber: Long, onceWhenLive: () -> Unit): Unit = lock.withLock {

        if (!isLive) {
            if (eventNumber >= lastEvent) {
                if (Instant.now().minus(liveCheckpointTimeout).isBefore(timestamp)) {
                    log.debug { "Subscription to stream $streamId became live at event number $eventNumber" }
                    isLive = true
                    onceWhenLive.invoke()
                } else {
                    lastEvent = eventStoreDBClient.getSubscriptionLiveCheckpoint(streamId)
                    timestamp = Instant.now()
                    log.debug { "Subscription reached expired live checkpoint. Setting new checkpoint for $streamId at $lastEvent" }
                    if (eventNumber >= lastEvent) {
                        log.debug { "Subscription to stream $streamId is live at event number $eventNumber" }
                        isLive = true
                        onceWhenLive.invoke()
                    }
                }
            }
        }
    }

}

private fun EventStoreDBClient.getSubscriptionLiveCheckpoint(streamId: String): Long {
    return try {
        readStream(
            streamId,
            ReadStreamOptions.get().maxCount(1).backwards().fromEnd().notResolveLinkTos()
        ).get().events.firstOrNull()?.originalEvent?.revision ?: -1L
    } catch (e: ExecutionException) {
        when (e.cause) {
            is StreamNotFoundException -> (-1L).also { log.debug(e.cause) { "Stream does not exist, returning -1 as last event number in $streamId" } }
            else -> throw e.cause!!
        }
    }
}

