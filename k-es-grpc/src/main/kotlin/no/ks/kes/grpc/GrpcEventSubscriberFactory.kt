package no.ks.kes.grpc

import com.eventstore.dbclient.*
import io.grpc.StatusRuntimeException
import mu.KotlinLogging
import no.ks.kes.grpc.GrpcEventUtil.isIgnorable
import no.ks.kes.grpc.GrpcEventUtil.isResolved
import no.ks.kes.grpc.GrpcSubscriptionDroppedReason.*
import no.ks.kes.lib.*
import no.ks.kes.lib.EventData
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong
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
        subscriber: String,
        fromEvent: Long,
        onEvent: (EventWrapper<EventData<*>>) -> Unit,
        onClose: (Exception) -> Unit,
        onLive: () -> Unit
    ): GrpcSubscriptionWrapper {

        val streamId = "\$ce-$category"
        //val revision = StreamRevision(fromEvent)
        // TODO: No way to start subscription from StreamRevision.END without knowing the number of events (same as for Esjc based client)
        val revision = when {
            fromEvent == -1L -> StreamRevision.START
            fromEvent > -1L -> StreamRevision(fromEvent)
            else -> error("the from-event $fromEvent is invalid, must be a number equal to or larger than -1")
        }

        val listener = object : SubscriptionListener() {

            var lastEventProcessed = AtomicLong(-1)

            override fun onEvent(subscription: Subscription, resolvedEvent: ResolvedEvent) {
                //if (event.getEvent().position == Position.END)
                // TODO: No way to say if we're live and should call onLive

                log.debug { "$subscriber: received event \"$resolvedEvent\"" }

                when {
                    !resolvedEvent.isResolved() ->
                        log.info { "$subscriber: event not resolved: ${resolvedEvent.link.streamRevision} ${resolvedEvent.link.streamId}" }
                    resolvedEvent.isIgnorable() ->
                        log.info { "$subscriber: event ignored: ${resolvedEvent.originalEvent.streamRevision} ${resolvedEvent.originalEvent.streamId}" }
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
                            eventNumber = resolvedEvent.originalEvent.streamRevision.valueUnsigned,
                            serializationId = serdes.getSerializationId(event::class)))
                            .also {
                                log.info("$subscriber: event ${resolvedEvent.originalEvent.streamRevision.valueUnsigned}@${resolvedEvent.originalEvent.streamId}: " +
                                        "${resolvedEvent.event.eventType}(${resolvedEvent.event.eventId}) received")
                            }
                    } catch (e: java.lang.Exception) {
                        log.error(e) { "Event handler for $subscriber threw exception: " }
                        throw e
                    }
                }

                lastEventProcessed.set(resolvedEvent.originalEvent.streamRevision.valueUnsigned)
            }

            override fun onCancelled(subscription: Subscription?) {
                log.error { "subscription cancelled. subscriptionId=${subscription?.subscriptionId}, subscriber=$subscriber, streamId=$streamId, lastEvent=$lastEventProcessed" }
                onClose.invoke(GrpcSubscriptionDroppedException(SubscriptionCancelled))
            }

            override fun onError(subscription: Subscription?, throwable: Throwable?) {
                // TODO: Figure out which exceptions we get and how to map to reason
                log.error { "error on subscription. subscriptionId=${subscription?.subscriptionId}, subscriber=$subscriber, streamId=$streamId, lastEvent=$lastEventProcessed, exception=$throwable" }
                when (throwable) {
                    is ConnectionShutdownException -> onClose.invoke(GrpcSubscriptionDroppedException(ConnectionShutDown, throwable))
                    is StatusRuntimeException -> onClose.invoke(GrpcSubscriptionDroppedException(GrpcStatusException, throwable))
                    else -> onClose.invoke(GrpcSubscriptionDroppedException(Unknown, RuntimeException(throwable)))
                }
            }
        }

        eventStoreDBClient.subscribeToStream(
            streamId,
            listener,
            SubscribeToStreamOptions.get()
                .resolveLinkTos()
                .fromRevision(revision)
        ).get()
        return GrpcSubscriptionWrapper(streamId) { listener.lastEventProcessed.get() }
    }

}

