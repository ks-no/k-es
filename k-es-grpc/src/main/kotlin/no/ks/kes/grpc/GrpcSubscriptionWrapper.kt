package no.ks.kes.grpc

import com.eventstore.dbclient.EventStoreDBClient
import com.eventstore.dbclient.StreamPosition
import com.eventstore.dbclient.SubscribeToStreamOptions
import com.eventstore.dbclient.Subscription
import mu.KotlinLogging
import no.ks.kes.lib.*
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicLong


private val log = KotlinLogging.logger {}
private const val MAX_RECONNECT_RETRIES = 10L

class GrpcSubscriptionWrapper(
    private val eventStoreDBClient: EventStoreDBClient,
    category: String,
    private val hwmId: String,
    fromEvent: Long,
    private val serdes: EventSerdes,
    private val metadataSerdes: EventMetadataSerdes<out Metadata>? = null,
    private val onEvent: (EventWrapper<EventData<*>>) -> Unit,
    private val onError: (Exception) -> Unit,
    private val onLive: () -> Unit
): EventSubscription {

    private val revision = when {
        fromEvent == -1L -> StreamPosition.start()
        fromEvent > -1L -> StreamPosition.position(fromEvent)
        else -> error("the from-event $fromEvent is invalid, must be a number equal to or larger than -1")
    }

    private val streamId = "\$ce-$category"
    private val subscriptionLiveCheckpoint = SubscriptionLiveCheckpoint(eventStoreDBClient, streamId)
    private val lastEventProcessed = AtomicLong(fromEvent)
    private val retryCount = AtomicLong(0)
    private var firstOnCancelled: Instant? = null
    private var subscription: Subscription = init()
    val isSubscribedToAll: Boolean
        get() = streamId == "\$all";

    override fun lastProcessedEvent(): Long = lastEventProcessed.get()

    fun subscriptionId() = subscription.subscriptionId


    private fun init(): Subscription =
        createListenerAndSubcription().also {
            // In case we are already live before we start receiving events.
            subscriptionLiveCheckpoint.triggerOnceIfSubscriptionIsLive(revision.position.orElse(-1)) {
                onLive.invoke()
            }
        }

    private fun createListenerAndSubcription(
    ) : Subscription {
        val listener = GrpcSubscriptionListener(
            streamId,
            hwmId,
            lastEventProcessed,
            onEvent,
            {
                onError(
                    it
                )
            },
            onLive,
            subscriptionLiveCheckpoint,
            serdes,
            metadataSerdes
        )
        return eventStoreDBClient.subscribeToStream(
            streamId,
            listener,
            SubscribeToStreamOptions.get()
                .resolveLinkTos()
                .fromRevision(revision)
        ).get().also { log.info("Subscription on stream '$streamId' created with subscriptionId '${it.subscriptionId}'") }
    }

    private fun onError(
        exception: Exception
    ) {
        if (retryCount.get() > 0 && firstOnCancelled?.isBefore(Instant.now().minus(3, ChronoUnit.MINUTES)) == true) {
            firstOnCancelled = null
            retryCount.set(0)
        }

        if (retryCount.get() >= MAX_RECONNECT_RETRIES) {
            log.error(exception) {"Error on subscription, automatic reconnect failed with ${retryCount.get()} attempts"}
            onError.invoke(exception)
        } else {
            log.info(exception) {"Error on subscription, automatic reconnect in ${retryCount.get()} seconds"}
            if (retryCount.get() == 0L) {
                firstOnCancelled = Instant.now()
            }

            Thread.sleep(Duration.ofSeconds(1).toMillis() * retryCount.getAndIncrement())

            subscription = createListenerAndSubcription()
        }
    }
}