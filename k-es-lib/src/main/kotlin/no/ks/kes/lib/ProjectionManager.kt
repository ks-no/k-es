package no.ks.kes.lib


class ProjectionManager(
        eventSubscriber: EventSubscriber,
        private val projections: Set<Projection>,
        private val hwmUpdater: (Long) -> Unit,
        private val onClose: (Exception) -> Unit
) {

    init {
        eventSubscriber.addSubscriber("ProjectionManager") { wrapper ->
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

