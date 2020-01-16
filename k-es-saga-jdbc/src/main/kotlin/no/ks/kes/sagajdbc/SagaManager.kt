package no.ks.kes.sagajdbc

import no.ks.kes.lib.*
import java.util.*

class SagaManager(eventSubscriber: EventSubscriber, sagaRepository: SagaRepository,  sagaSerdes: SagaSerdes, sagas: Set<Saga<*>>) {
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
                                    saga.initializer.handler(e)
                                            ?.let {
                                        NewSagaState(
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
            sagaRepository.update(subscriptions[event.event::class]?.map { it.second.invoke(event) }
            sagaRepository.updateHwm(event.eventNumber)
        }
    }

    data class NewSagaState(val correlationId: UUID, val sagaSerializationId: String, val data: ByteArray, val commands: List<Cmd<*>>)

}

