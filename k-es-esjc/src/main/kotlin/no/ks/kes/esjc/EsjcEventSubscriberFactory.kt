package no.ks.kes.esjc

import com.github.msemys.esjc.*
import mu.KotlinLogging
import no.ks.kes.lib.*
import no.ks.kes.lib.EventData
import java.util.*
import kotlin.reflect.KClass
import java.lang.Exception as JavaException

private val log = KotlinLogging.logger {}

class EsjcEventSubscriberFactory(
        private val eventStore: EventStore,
        private val serdes: EventSerdes,
        private val category: String,
        private val metadataSerdes: EventMetadataSerdes<out Metadata>? = null
) : EventSubscriberFactory<CatchUpSubscriptionWrapper> {
    override fun getSerializationId(eventDataClass: KClass<EventData<*>>): String =
            serdes.getSerializationId(eventDataClass)

    override fun createSubscriber(hwmId: String, fromEvent: Long, onEvent: (EventWrapper<EventData<*>>) -> Unit, onLive: () -> Unit, onError: (JavaException) -> Unit): CatchUpSubscriptionWrapper =
            CatchUpSubscriptionWrapper(eventStore.subscribeToStreamFrom(
                    "\$ce-$category",
                    when {
                        fromEvent == -1L -> null
                        fromEvent > -1L -> fromEvent
                        else -> error("the from-event $fromEvent is invalid, must be a number equal to or larger than -1")
                    },
                    CatchUpSubscriptionSettings.newBuilder().resolveLinkTos(true).build(),
                    object : CatchUpSubscriptionListener {
                        override fun onClose(subscription: CatchUpSubscription, reason: SubscriptionDropReason, exception: JavaException) {
                            log.error(exception) { "$hwmId: subscription closed: $reason" }
                            onError.invoke(EsjcSubscriptionDroppedException(reason, exception))
                        }

                        override fun onLiveProcessingStarted(subscription: CatchUpSubscription?) {
                            log.info { "$hwmId: subscription is live!" }
                            onLive.invoke()
                        }

                        override fun onEvent(subscription: CatchUpSubscription?, resolvedEvent: ResolvedEvent?) {
                            log.debug { "$hwmId: received event \"$resolvedEvent\"" }
                            when {
                                !resolvedEvent!!.isResolved ->
                                    log.info { "$hwmId: event not resolved: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}" }
                                EsjcEventUtil.isIgnorableEvent(resolvedEvent) ->
                                    log.info { "$hwmId: event ignored: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}" }
                                else -> try {
                                    val eventMeta = if(resolvedEvent.event.metadata.isNotEmpty() && metadataSerdes != null) metadataSerdes.deserialize(resolvedEvent.event.metadata) else null
                                    val event = EventUpgrader.upgrade(serdes.deserialize(resolvedEvent.event.data, resolvedEvent.event.eventType))
                                    val aggregateId = UUID.fromString(resolvedEvent.event.eventStreamId.takeLast(36))
                                    onEvent.invoke(EventWrapper(
                                        Event(
                                            aggregateId = aggregateId,
                                            metadata = eventMeta,
                                            eventData = event
                                        ),
                                            eventNumber = resolvedEvent.originalEventNumber(),
                                            serializationId = serdes.getSerializationId(event::class)))
                                            .also {
                                                log.info("$hwmId: event ${resolvedEvent.originalEventNumber()}@${resolvedEvent.originalStreamId()}: " +
                                                        "${resolvedEvent.event.eventType}(${resolvedEvent.event.eventId}) received")
                                            }
                                } catch (e: Exception) {
                                    log.error(e) { "Event handler for $hwmId threw exception: " }
                                    throw e
                                }
                            }
                        }
                    }
            ).also {
                log.info { "$hwmId: subscription initiated from event number $fromEvent on category $category" }
            })

}