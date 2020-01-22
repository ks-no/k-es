package no.ks.kes.esjc

import com.github.msemys.esjc.CatchUpSubscriptionSettings
import com.github.msemys.esjc.EventStore
import mu.KotlinLogging
import no.ks.kes.lib.Event
import no.ks.kes.lib.EventSerdes
import no.ks.kes.lib.EventSubscriber
import no.ks.kes.lib.EventWrapper

private val log = KotlinLogging.logger {}

class EsjcEventSubscriber(
        private val eventStore: EventStore,
        private val serdes: EventSerdes<String>,
        private val fromEvent: Long,
        private val category: String
) : EventSubscriber {
    private var onClose: (Exception) -> Unit = {}
    private var onLive: () -> Unit = {}

    override fun addSubscriber(consumerName: String, consumer: (EventWrapper<Event<*>>) -> Unit) {
        eventStore.subscribeToStreamFrom(
                "\$ce-$category",
                fromEvent,
                CatchUpSubscriptionSettings.newBuilder().resolveLinkTos(true).build()
        ) { _, resolvedEvent ->
            when {
                !resolvedEvent.isResolved ->
                    log.info("$consumerName: event not resolved: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}")
                EsjcEventUtil.isIgnorableEvent(resolvedEvent) ->
                    log.info("$consumerName: event ignored: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}")
                else ->
                    consumer.invoke(EventWrapper(
                            event = serdes.deserialize(String(resolvedEvent.event.data), resolvedEvent.event.eventType),
                            eventNumber = resolvedEvent.originalEventNumber()))
                            .also {
                                log.info("$consumerName: event ${resolvedEvent.originalEventNumber()}@${resolvedEvent.originalStreamId()}: " +
                                        "${resolvedEvent.event.eventType}(${resolvedEvent.event.eventId}) received")
                            }
            }

        }.also {
            log.info("$consumerName: subscription initiated from event number $fromEvent on category $category")
        }!!
    }

    override fun onClose(handler: (Exception) -> Unit) {
        this.onClose = handler
    }

    override fun onLive(handler: () -> Unit) {
       this.onLive = handler
    }

}