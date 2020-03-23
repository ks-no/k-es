package no.ks.kes.lib


object Projections {
    fun initialize(
            eventSubscriberFactory: EventSubscriberFactory,
            projections: Set<Projection>,
            projectionRepository: ProjectionRepository,
            subscriber: String,
            onClose: (Exception) -> Unit = {}
    ) {
        eventSubscriberFactory.createSubscriber(
                subscriber = subscriber,
                onEvent = { wrapper ->
                    projectionRepository.transactionally {
                        projections.forEach {
                            it.accept(
                                    EventWrapper(
                                            event = wrapper.event,
                                            eventNumber = wrapper.eventNumber
                                    )
                            )
                            projectionRepository.hwmTracker.update(subscriber, wrapper.eventNumber)
                        }
                    }
                },
                fromEvent = projectionRepository.hwmTracker.getOrInit(subscriber),
                onClose = { onClose.invoke(it) },
                onLive = { projections.forEach { it.onLive() } }
        )
    }
}