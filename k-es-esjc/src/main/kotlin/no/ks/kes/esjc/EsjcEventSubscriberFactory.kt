package no.ks.kes.esjc

import com.github.msemys.esjc.*
import mu.KotlinLogging
import no.ks.kes.lib.*
import kotlin.reflect.KClass
import java.lang.Exception as JavaException

private val log = KotlinLogging.logger {}

class EsjcEventSubscriberFactory(
        private val eventStore: EventStore,
        private val serdes: EventSerdes,
        private val category: String,
        private val eventMetadataSerdes: EventMetadataSerdes? = null
) : EventSubscriberFactory<CatchUpSubscriptionWrapper> {
    override fun getSerializationId(eventClass: KClass<Event<*>>): String =
            serdes.getSerializationId(eventClass)

    override fun createSubscriber(subscriber: String, fromEvent: Long, onEvent: (EventWrapper<Event<*>>) -> Unit, onClose: (JavaException) -> Unit, onLive: () -> Unit): CatchUpSubscriptionWrapper =
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
                            log.error(exception) { "$subscriber: subscription closed: $reason" }
                            onClose.invoke(EsjcSubscriptionDroppedException(reason, exception))
                        }

                        override fun onLiveProcessingStarted(subscription: CatchUpSubscription?) {
                            log.info { "$subscriber: subscription is live!" }
                            onLive.invoke()
                        }

                        override fun onEvent(subscription: CatchUpSubscription?, resolvedEvent: ResolvedEvent?) {
                            log.debug { "$subscriber: received event \"$resolvedEvent\"" }
                            when {
                                !resolvedEvent!!.isResolved ->
                                    log.info { "$subscriber: event not resolved: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}" }
                                EsjcEventUtil.isIgnorableEvent(resolvedEvent) ->
                                    log.info { "$subscriber: event ignored: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}" }
                                else -> try {
                                    val eventMeta = if(resolvedEvent.event.metadata.isNotEmpty() && eventMetadataSerdes != null) eventMetadataSerdes.deserialize(resolvedEvent.event.metadata,resolvedEvent.event.eventType) else EventMetadata()
                                    val event = EventUpgrader.upgrade(serdes.deserialize(eventMeta, resolvedEvent.event.data, resolvedEvent.event.eventType))
                                    onEvent.invoke(EventWrapper(
                                            event = event,
                                            eventNumber = resolvedEvent.originalEventNumber(),
                                            serializationId = serdes.getSerializationId(event::class)))
                                            .also {
                                                log.info("$subscriber: event ${resolvedEvent.originalEventNumber()}@${resolvedEvent.originalStreamId()}: " +
                                                        "${resolvedEvent.event.eventType}(${resolvedEvent.event.eventId}) received")
                                            }
                                } catch (e: Exception) {
                                    log.error(e) { "Event handler for $subscriber threw exception: " }
                                    throw e
                                }
                            }
                        }
                    }
            ).also {
                log.info { "$subscriber: subscription initiated from event number $fromEvent on category $category" }
            })

}