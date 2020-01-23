package no.ks.kes.esjc

import com.github.msemys.esjc.*
import mu.KotlinLogging
import no.ks.kes.lib.Event
import no.ks.kes.lib.EventSerdes
import no.ks.kes.lib.EventSubscriber
import no.ks.kes.lib.EventWrapper

private val log = KotlinLogging.logger {}

class EsjcEventSubscriber(
        private val eventStore: EventStore,
        private val serdes: EventSerdes<String>,
        private val category: String
) : EventSubscriber {

    override fun addSubscriber(consumerName: String, fromEvent: Long, onEvent: (EventWrapper<Event<*>>) -> Unit, onClose: (Exception) -> Unit, onLive: () -> Unit) {
        eventStore.subscribeToStreamFrom(
                "\$ce-$category",
                fromEvent,
                CatchUpSubscriptionSettings.newBuilder().resolveLinkTos(true).build(),
                object : CatchUpSubscriptionListener {
                    override fun onClose(subscription: CatchUpSubscription, reason: SubscriptionDropReason, exception: java.lang.Exception) {
                        log.error("$consumerName: subscription closed: $reason", exception)
                        onClose.invoke(exception)
                    }

                    override fun onLiveProcessingStarted(subscription: CatchUpSubscription?) {
                        log.error("$consumerName: subscription is live!")
                        onLive.invoke()
                    }

                    override fun onEvent(p0: CatchUpSubscription, resolvedEvent: ResolvedEvent) {
                        when {
                            !resolvedEvent.isResolved ->
                                log.info("$consumerName: event not resolved: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}")
                            EsjcEventUtil.isIgnorableEvent(resolvedEvent) ->
                                log.info("$consumerName: event ignored: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}")
                            else ->
                                onEvent.invoke(EventWrapper(
                                        event = serdes.deserialize(String(resolvedEvent.event.data), resolvedEvent.event.eventType),
                                        eventNumber = resolvedEvent.originalEventNumber()))
                                        .also {
                                            log.info("$consumerName: event ${resolvedEvent.originalEventNumber()}@${resolvedEvent.originalStreamId()}: " +
                                                    "${resolvedEvent.event.eventType}(${resolvedEvent.event.eventId}) received")
                                        }
                        }
                    }
                }
        )
                .also {
                    log.info("$consumerName: subscription initiated from event number $fromEvent on category $category")
                }!!
    }

}