package no.ks.kes.lib


object Projections {
    fun initialize(
            eventSubscriberFactory: EventSubscriberFactory,
            projections: Set<Projection>,
            projectionRepository: ProjectionRepository,
            subscriber: String,
            onClose: (Exception) -> Unit = {}
    ) {
        val validatedProjectionConfigurations = projections.map { projection -> projection.getConfiguration { eventSubscriberFactory.getSerializationId(it) } }

        eventSubscriberFactory.createSubscriber(
                subscriber = subscriber,
                onEvent = { wrapper ->
                    projectionRepository.transactionally {
                        validatedProjectionConfigurations.forEach {
                            it.accept(wrapper)
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