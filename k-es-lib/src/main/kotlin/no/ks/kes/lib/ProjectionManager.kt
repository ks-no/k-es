package no.ks.kes.lib

import mu.KotlinLogging


private val log = KotlinLogging.logger {}

class ProjectionManager(
        private val projections: Set<Projection>,
        private val hwmUpdater: (Long) -> Unit,
        private val onClose: (Exception) -> Unit,
        eventSubscriber: EventSubscriber
) {

    init {
        eventSubscriber.subscribe { wrapper ->
            projections.forEach {
                it.accept(EventWrapper(
                        event = wrapper.event,
                        eventNumber = wrapper.eventNumber))
            }
                    .also { hwmUpdater.invoke(wrapper.eventNumber) }
        }

        eventSubscriber.onClose { this.onClose.invoke(it) }
        eventSubscriber.onLive { projections.forEach { it.onLive() } }
    }

}

