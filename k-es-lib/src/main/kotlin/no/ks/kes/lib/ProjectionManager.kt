package no.ks.kes.lib


class ProjectionManager(
        eventSubscriber: EventSubscriber,
        projections: Set<Projection>,
        fromEvent: Long,
        hwmUpdater: (Long) -> Unit,
        onClose: (Exception) -> Unit
) {
    init {
        eventSubscriber.addSubscriber(
                consumerName = "ProjectionManager",
                onEvent = { wrapper ->
                    projections.forEach {
                        it.accept(EventWrapper(
                                event = wrapper.event,
                                eventNumber = wrapper.eventNumber))
                    }
                            .also { hwmUpdater.invoke(wrapper.eventNumber) }
                },
                fromEvent = fromEvent,
                onClose = { onClose.invoke(it) },
                onLive = { projections.forEach { it.onLive() } }
        )
    }
}