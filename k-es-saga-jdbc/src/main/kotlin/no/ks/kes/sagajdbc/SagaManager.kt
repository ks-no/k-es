package no.ks.kes.sagajdbc

import no.ks.kes.lib.*

class SagaManager(eventSubscriber: EventSubscriber, private val sagaRepository: SagaRepository, private val sagaSerdes: SagaSerdes, sagas: Set<Saga<*>>) {
    private val subscriptions = sagas
            .map { it.getConfiguration() }
            .flatMap { saga ->
                saga.onEvents
                        .map { onEvent ->
                            onEvent.eventClass to { e: EventWrapper<Event<*>> ->
                                sagaRepository.get(onEvent.correlationId(e), AnnotationUtil.getSagaType(saga.sagaClass))
                                        ?.let {
                                            onEvent.handler.invoke(e, sagaSerdes.deserialize(it, saga.stateClass))
                                        }
                                        ?.apply {
                                            sagaRepository.save(
                                                    correlationId = onEvent.correlationId(e),
                                                    sagaSerializationId = AnnotationUtil.getSagaType(saga.sagaClass),
                                                    data = sagaSerdes.serialize(this)
                                            )
                                        }
                            }
                        }
                        .plus(
                                saga.initializer.eventClass to { e: EventWrapper<Event<*>> ->
                                    saga.initializer.handler(e)?.apply {
                                        sagaRepository.save(
                                                correlationId = saga.initializer.correlationId(e),
                                                sagaSerializationId = AnnotationUtil.getSagaType(saga.sagaClass),
                                                data = sagaSerdes.serialize(this)
                                        )
                                    }
                                }
                        )
            }
            .groupBy { it.first }

    init {
        eventSubscriber.subscribe(sagaRepository.getCurrentHwm()) { event ->
            subscriptions[event.event::class]?.forEach { it.second.invoke(event) }
            sagaRepository.updateHwm(event.eventNumber)
        }
    }

}

