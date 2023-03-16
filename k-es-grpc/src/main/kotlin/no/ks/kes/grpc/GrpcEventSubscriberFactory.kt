package no.ks.kes.grpc

import com.eventstore.dbclient.*
import com.eventstore.dbclient.StreamPosition
import mu.KotlinLogging
import no.ks.kes.lib.*
import no.ks.kes.lib.EventData
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

private const val MAX_RECONNECT_RETRIES = 10L

class GrpcEventSubscriberFactory(
    private val eventStoreDBClient: EventStoreDBClient,
    private val serdes: EventSerdes,
    private val category: String,
    private val metadataSerdes: EventMetadataSerdes<out Metadata>? = null
) : EventSubscriberFactory<GrpcSubscriptionWrapper> {

    private val retryCount = AtomicLong(0)
    private var firstOnCancelled: Instant? = null

    override fun getSerializationId(eventDataClass: KClass<EventData<*>>): String =
        serdes.getSerializationId(eventDataClass)

    private fun onCancelled(
        streamId: String,
        hwmId: String,
        lastEventProcessed: AtomicLong,
        onEvent: (EventWrapper<EventData<*>>) -> Unit,
        onError: (Exception) -> Unit,
        onLive: () -> Unit,
        subscriptionLiveCheckpoint: SubscriptionLiveCheckpoint,
        revision: StreamPosition<Long>?,
        exception: Exception
    ) {
        log.info(exception) {"Subscription canceled, automatic reconnect in ${retryCount.get()} seconds"}
        if (retryCount.get() > 0 && firstOnCancelled?.isBefore(Instant.now().minus(3, ChronoUnit.MINUTES)) == true) {
            firstOnCancelled = null
            retryCount.set(0)
        }

        if (retryCount.get() >= MAX_RECONNECT_RETRIES) {
            log.error(exception) {"Subscription canceled, automatic reconnect failed with ${retryCount.get()} attempts"}
            onError.invoke(GrpcSubscriptionDroppedException(GrpcSubscriptionDroppedReason.SubscriptionDroped, RuntimeException(exception)))
        } else {

            if (retryCount.get() == 0L) {
                firstOnCancelled = Instant.now()
            }

            Thread.sleep(Duration.ofSeconds(1).toMillis() * retryCount.getAndIncrement())

            createListenerAndSubcription(
                streamId,
                hwmId,
                lastEventProcessed,
                onEvent,
                onError,
                onLive,
                subscriptionLiveCheckpoint,
                revision
            )
        }
    }

    override fun createSubscriber(
        hwmId: String,
        fromEvent: Long,
        onEvent: (EventWrapper<EventData<*>>) -> Unit,
        onLive: () -> Unit,
        onError: (Exception) -> Unit
    ): GrpcSubscriptionWrapper {

        val streamId = "\$ce-$category"
        val lastEventProcessed = AtomicLong(fromEvent)

        val revision = when {
            fromEvent == -1L -> StreamPosition.start()
            fromEvent > -1L -> StreamPosition.position(fromEvent)
            else -> error("the from-event $fromEvent is invalid, must be a number equal to or larger than -1")
        }

        val subscriptionLiveCheckpoint = SubscriptionLiveCheckpoint(eventStoreDBClient, streamId)

        createListenerAndSubcription(
            streamId,
            hwmId,
            lastEventProcessed,
            onEvent,
            onError,
            onLive,
            subscriptionLiveCheckpoint,
            revision
        )

        // In case we are already live before we start receiving events.
        subscriptionLiveCheckpoint.triggerOnceIfSubscriptionIsLive(revision.position.orElse(-1)) {
            onLive.invoke()
        }

        return GrpcSubscriptionWrapper(streamId) { lastEventProcessed.get() }
    }

    private fun createListenerAndSubcription(
        streamId: String,
        hwmId: String,
        lastEventProcessed: AtomicLong,
        onEvent: (EventWrapper<EventData<*>>) -> Unit,
        onError: (Exception) -> Unit,
        onLive: () -> Unit,
        subscriptionLiveCheckpoint: SubscriptionLiveCheckpoint,
        revision: StreamPosition<Long>?
    ) {
        val listener = GrpcSubscriptionListener(
            streamId,
            hwmId,
            lastEventProcessed,
            onEvent,
            {
                onCancelled(
                    streamId,
                    hwmId,
                    lastEventProcessed,
                    onEvent,
                    onError,
                    onLive,
                    subscriptionLiveCheckpoint,
                    revision,
                    it
                )
            },
            onLive,
            subscriptionLiveCheckpoint,
            serdes,
            metadataSerdes
        )
        eventStoreDBClient.subscribeToStream(
            streamId,
            listener,
            SubscribeToStreamOptions.get()
                .resolveLinkTos()
                .fromRevision(revision)
        ).get()
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

