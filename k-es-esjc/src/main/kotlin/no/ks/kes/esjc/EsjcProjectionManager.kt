package no.ks.kes.esjc

import com.github.msemys.esjc.CatchUpSubscription
import com.github.msemys.esjc.CatchUpSubscriptionListener
import com.github.msemys.esjc.ResolvedEvent
import com.github.msemys.esjc.SubscriptionDropReason
import mu.KotlinLogging
import no.ks.kes.lib.EventListener
import no.ks.kes.lib.EventSerdes
import no.ks.kes.lib.EventWrapper
import no.ks.kes.lib.Projection


private val log = KotlinLogging.logger {}

class EsjcEventListener(
        private val deserializer: EventSerdes,
        private val projections: Set<Projection>,
        private val hwmUpdater: (Long) -> Unit,
        private val onClose: (SubscriptionDropReason, Exception) -> Unit
) : CatchUpSubscriptionListener {

    override fun onEvent(catchUpSubscription: CatchUpSubscription, resolvedEvent: ResolvedEvent) =
            when {
                !resolvedEvent.isResolved ->
                    log.info("Event not resolved: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}")
                EsjcEventUtil.isIgnorableEvent(resolvedEvent) ->
                    log.info("Event ignored: ${resolvedEvent.originalEventNumber()} ${resolvedEvent.originalStreamId()}")
                else ->
                    projections.forEach {
                        it.accept(EventWrapper(
                                event = deserializer.deserialize(resolvedEvent.event.data, resolvedEvent.event.eventType),
                                eventNumber = resolvedEvent.originalEventNumber()))
                    }
                            .also { hwmUpdater.invoke(resolvedEvent.originalEventNumber()) }
                            .also {
                                log.info("Event ${resolvedEvent.originalEventNumber()}@${resolvedEvent.originalStreamId()}: " +
                                        "${resolvedEvent.event.eventType}(${resolvedEvent.event.eventId}) received")
                            }

            }

    override fun onClose(subscription: CatchUpSubscription, reason: SubscriptionDropReason, exception: Exception) {
        super.onClose(subscription, reason, exception)
        log.error("I'm dead: $reason", exception)
        this.onClose.invoke(reason, exception)
    }

    override fun onLiveProcessingStarted(subscription: CatchUpSubscription) {
        log.info("We're live!")
        projections.forEach { it.onLive() }
    }

}