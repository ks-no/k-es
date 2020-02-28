package no.ks.kes.lib


object Projections {
    fun initialize(
            eventSubscriber: EventSubscriber,
            projections: Set<Projection>,
            projectionRepository: ProjectionRepository,
            onClose: (Exception) -> Unit = {},
            consumerName: String) {
        eventSubscriber.addSubscriber(
                consumerName = consumerName,
                onEvent = { wrapper ->
                    projectionRepository.transactionally {
                        projections.forEach {
                            it.accept(EventWrapper(
                                    event = wrapper.event,
                                    eventNumber = wrapper.eventNumber))
                        }
                                .also { projectionRepository.updateHwm(wrapper.eventNumber, consumerName) }
                    }
                },
                fromEvent = projectionRepository.currentHwm(consumerName),
                onClose = { onClose.invoke(it) },
                onLive = { projections.forEach { it.onLive() } }
        )
    }
}