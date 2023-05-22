package no.ks.kes.grpc

import com.eventstore.dbclient.*
import no.ks.kes.grpc.GrpcEventUtil.isIgnorable
import no.ks.kes.grpc.GrpcEventUtil.isResolved
import no.ks.kes.lib.*
import no.ks.kes.lib.EventData
import java.util.*
import java.util.concurrent.atomic.AtomicLong

private val log = mu.KotlinLogging.logger {  }
class GrpcSubscriptionListener (private val streamId: String,
                                private val hwmId: String,
                                private val lastEventProcessed: AtomicLong,
                                private val onEvent: (EventWrapper<EventData<*>>) -> Unit,
                                private val onError: (Exception) -> Unit,
                                private val onLive: () -> Unit,
                                private val subscriptionLiveCheckpoint: SubscriptionLiveCheckpoint,
                                private val serdes: EventSerdes,
                                private val metadataSerdes: EventMetadataSerdes<out Metadata>? = null
) : SubscriptionListener() {

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
                val eventMeta =
                    if (resolvedEvent.event.userMetadata.isNotEmpty() && metadataSerdes != null) metadataSerdes.deserialize(
                        resolvedEvent.event.userMetadata
                    ) else null
                val event = EventUpgrader.upgrade(
                    serdes.deserialize(
                        resolvedEvent.event.eventData,
                        resolvedEvent.event.eventType
                    )
                )
                val aggregateId = UUID.fromString(resolvedEvent.event.streamId.takeLast(36))
                onEvent.invoke(
                    EventWrapper(
                        Event(
                            aggregateId = aggregateId,
                            metadata = eventMeta,
                            eventData = event
                        ),
                        eventNumber = eventNumber,
                        serializationId = serdes.getSerializationId(event::class)
                    )
                )
                    .also {
                        log.debug(
                            "$hwmId: event ${eventNumber}@${resolvedEvent.originalEvent.streamId}: " +
                                    "${resolvedEvent.event.eventType}(${resolvedEvent.event.eventId}) received"
                        )
                    }
            } catch (e: java.lang.Exception) {
                log.error(e) { "Event handler for $hwmId threw exception: " }
                throw e
            }
        }

        lastEventProcessed.set(eventNumber)
    }

    override fun onCancelled(subscription: Subscription?) {
        log.info { "Subscription cancelled: subscriptionId=${subscription?.subscriptionId}, hwmId=$hwmId, streamId=$streamId, lastEvent=$lastEventProcessed" }
    }

    override fun onError(subscription: Subscription?, throwable: Throwable?) {
        if(throwable?.message.contentEquals(other = "Consumer too slow", ignoreCase = true)){
            log.info { "Fikk 'Consumer too slow' på subscriptionId=${subscription?.subscriptionId}, streamId=$streamId" }
        } else {
            log.error(throwable) { "error on subscription. subscriptionId=${subscription?.subscriptionId}, hwmId=$hwmId, streamId=$streamId, lastEvent=$lastEventProcessed" }
        }

        when (throwable) {
            is ConnectionShutdownException -> onError.invoke(GrpcSubscriptionException(
                GrpcSubscriptionExceptionReason.ConnectionShutDown, throwable))
            else -> onError.invoke(GrpcSubscriptionException(GrpcSubscriptionExceptionReason.Unknown, RuntimeException(throwable)))
        }
    }
}