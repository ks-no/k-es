package no.ks.kes.esjc

import com.github.msemys.esjc.*
import mu.KotlinLogging
import no.ks.kes.lib.Event
import no.ks.kes.lib.EventSerdes
import no.ks.kes.lib.EventSubscriber
import no.ks.kes.lib.EventWrapper

private val log = KotlinLogging.logger {}

class EsjcEventSubscriber(private val eventStore: EventStore, private val category: String, private val hwm: Long, private val serdes: EventSerdes): EventSubscriber {
    override fun subscribe(consumer: (EventWrapper<Event<*>>) -> Unit) {
        eventStore.subscribeToStreamFrom(
                "\$ce-$category",
                hwm,
                CatchUpSubscriptionSettings.newBuilder().resolveLinkTos(true).build()
        ) { subscription, resolvedEvent ->
            when {
                !resolvedEvent.isResolved ->
                    log.info("Event not resolved: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}")
                EsjcEventUtil.isIgnorableEvent(resolvedEvent) ->
                    log.info("Event ignored: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}")
                else ->
                        consumer.invoke(EventWrapper(
                                event = serdes.deserialize(resolvedEvent.event.data, resolvedEvent.event.eventType),
                                eventNumber = resolvedEvent.originalEventNumber()))
                            .also {
                                log.info("Event ${resolvedEvent.originalEventNumber()}@${resolvedEvent.originalStreamId()}: " +
                                        "${resolvedEvent.event.eventType}(${resolvedEvent.event.eventId}) received")
                            }
            }

        }.also {
            log.info("Subscription initiated from event number $hwm on category projection $category")
        }!!
    }
}