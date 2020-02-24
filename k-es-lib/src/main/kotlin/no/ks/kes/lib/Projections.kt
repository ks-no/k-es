package no.ks.kes.lib


object Projections {
    fun initialize(
            eventSubscriber: EventSubscriber,
            projections: Set<Projection>,
            projectionRepository: ProjectionRepository,
            onClose: (Exception) -> Unit = {}) {
        eventSubscriber.addSubscriber(
                consumerName = "ProjectionManager",
                onEvent = { wrapper ->
                    projections.forEach {
                        it.accept(EventWrapper(
                                event = wrapper.event,
                                eventNumber = wrapper.eventNumber))
                    }
                            .also { projectionRepository.updateHwm(wrapper.eventNumber) }
                },
                fromEvent = projectionRepository.currentHwm(),
                onClose = { onClose.invoke(it) },
                onLive = { projections.forEach { it.onLive() } }
        )
    }
}