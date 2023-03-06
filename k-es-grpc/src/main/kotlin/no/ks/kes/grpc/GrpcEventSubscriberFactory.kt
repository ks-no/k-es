package no.ks.kes.grpc

import com.eventstore.dbclient.*
import com.eventstore.dbclient.StreamPosition
import mu.KotlinLogging
import no.ks.kes.grpc.GrpcEventUtil.isIgnorable
import no.ks.kes.grpc.GrpcEventUtil.isResolved
import no.ks.kes.grpc.GrpcSubscriptionDroppedReason.*
import no.ks.kes.lib.*
import no.ks.kes.lib.EventData
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

class GrpcEventSubscriberFactory(
    private val eventStoreDBClient: EventStoreDBClient,
    private val serdes: EventSerdes,
    private val category: String,
    private val metadataSerdes: EventMetadataSerdes<out Metadata>? = null
) : EventSubscriberFactory<GrpcSubscriptionWrapper> {

    override fun getSerializationId(eventDataClass: KClass<EventData<*>>): String =
        serdes.getSerializationId(eventDataClass)

    override fun createSubscriber(
        hwmId: String,
        fromEvent: Long,
        onEvent: (EventWrapper<EventData<*>>) -> Unit,
        onClose: (Exception) -> Unit,
        onLive: () -> Unit
    ): GrpcSubscriptionWrapper {

        val streamId = "\$ce-$category"
        val revision = when {
            fromEvent == -1L -> StreamPosition.start()
            fromEvent > -1L -> StreamPosition.position(fromEvent)
            else -> error("the from-event $fromEvent is invalid, must be a number equal to or larger than -1")
        }

        val subscriptionLiveCheckpoint = SubscriptionLiveCheckpoint(eventStoreDBClient, streamId)

        val listener = object : SubscriptionListener() {

            var lastEventProcessed = AtomicLong(fromEvent)

            override fun onEvent(subscription: Subscription, resolvedEvent: ResolvedEvent) {

                log.debug { "$hwmId: received event \"$resolvedEvent\"" }

                val eventNumber = resolvedEvent.originalEvent.revision

                subscriptionLiveCheckpoint.triggerOnceIfSubscriptionIsLive(eventNumber) {
                    onLive.invoke()
                }

                when {
                    !resolvedEvent.isResolved() ->
                        log.info { "$hwmId: event not resolved: ${resolvedEvent.link.revision} ${resolvedEvent.link.streamId}" }
                    resolvedEvent.isIgnorable() ->
                        log.info { "$hwmId: event ignored: ${resolvedEvent.originalEvent.revision} ${resolvedEvent.originalEvent.streamId}" }
                    else -> try {
                        val eventMeta = if(resolvedEvent.event.userMetadata.isNotEmpty() && metadataSerdes != null) metadataSerdes.deserialize(resolvedEvent.event.userMetadata) else null
                        val event = EventUpgrader.upgrade(serdes.deserialize(resolvedEvent.event.eventData, resolvedEvent.event.eventType))
                        val aggregateId = UUID.fromString(resolvedEvent.event.streamId.takeLast(36))
                        onEvent.invoke(EventWrapper(
                            Event(
                                aggregateId = aggregateId,
                                metadata = eventMeta,
                                eventData = event
                            ),
                            eventNumber = eventNumber,
                            serializationId = serdes.getSerializationId(event::class)))
                            .also {
                                log.info("$hwmId: event ${eventNumber}@${resolvedEvent.originalEvent.streamId}: " +
                                        "${resolvedEvent.event.eventType}(${resolvedEvent.event.eventId}) received")
                            }
                    } catch (e: java.lang.Exception) {
                        log.error(e) { "Event handler for $hwmId threw exception: " }
                        throw e
                    }
                }

                lastEventProcessed.set(eventNumber)
            }

            override fun onCancelled(subscription: Subscription?) {
                log.error { "subscription cancelled. subscriptionId=${subscription?.subscriptionId}, subscriber=$hwmId, streamId=$streamId, lastEvent=$lastEventProcessed" }
                onClose.invoke(GrpcSubscriptionDroppedException(SubscriptionCancelled))
            }

            override fun onError(subscription: Subscription?, throwable: Throwable?) {
                log.error { "error on subscription. subscriptionId=${subscription?.subscriptionId}, subscriber=$hwmId, streamId=$streamId, lastEvent=$lastEventProcessed, exception=$throwable" }
                when (throwable) {
                    is ConnectionShutdownException -> onClose.invoke(GrpcSubscriptionDroppedException(ConnectionShutDown, throwable))
                    else -> onClose.invoke(GrpcSubscriptionDroppedException(Unknown, RuntimeException(throwable)))
                }
            }
        }

        val subscription = eventStoreDBClient.subscribeToStream(
            streamId,
            listener,
            SubscribeToStreamOptions.get()
                .resolveLinkTos()
                .fromRevision(revision)
        ).get()

        // In case we are already live before we start receiving events.
        subscriptionLiveCheckpoint.triggerOnceIfSubscriptionIsLive(revision.position.orElse(-1)) {
            onLive.invoke()
        }

        return GrpcSubscriptionWrapper(streamId, subscription) { listener.lastEventProcessed.get() }
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

