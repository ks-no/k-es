package no.ks.kes.lib

class SagaManager(eventSubscriber: EventSubscriber, sagaRepository: SagaRepository, sagaSerdes: SagaSerdes, sagas: Set<Saga<*>>) {
    private val subscriptions = sagas
            .map { it.getConfiguration() }
            .flatMap { saga ->
                saga.onEvents
                        .map { onEvent ->
                            onEvent.eventClass to { e: EventWrapper<Event<*>> ->
                                sagaRepository.get(onEvent.correlationId(e), AnnotationUtil.getSagaType(saga.sagaClass))
                                        ?.let {
                                            onEvent.handler.invoke(e, Saga.SagaContext(sagaSerdes.deserialize(it, saga.stateClass)))
                                        }
                                        ?.let {
                                            if (it.first != null || it.second.isNotEmpty())
                                                SagaRepository.NewSagaState(
                                                        correlationId = onEvent.correlationId(e),
                                                        sagaSerializationId = AnnotationUtil.getSagaType(saga.sagaClass),
                                                        data = if (it.first != null) sagaSerdes.serialize(it.first!!) else null,
                                                        commands = it.second
                                                )
                                            else
                                                null
                                        }
                            }
                        }
                        .plus(
                                saga.initializer.eventClass to { e: EventWrapper<Event<*>> ->
                                    saga.initializer.handler(e, Saga.InitContext())
                                            .let {
                                                if (it.first != null)
                                                    SagaRepository.NewSagaState(
                                                            correlationId = saga.initializer.correlationId(e),
                                                            sagaSerializationId = AnnotationUtil.getSagaType(saga.sagaClass),
                                                            data = sagaSerdes.serialize(it.first!!),
                                                            commands = it.second
                                                    )
                                                else
                                                    null
                                            }
                                }
                        )
            }
            .groupBy { it.first }

    init {
        eventSubscriber.subscribe { event ->
            subscriptions[event.event::class]
                    ?.mapNotNull { it.second.invoke(event) }
                    ?.toSet()
                    ?.apply {
                        sagaRepository.update(event.eventNumber, this)
                    }
        }
    }
}

