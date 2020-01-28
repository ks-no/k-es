package no.ks.kes.lib

import mu.KotlinLogging
import java.util.*


private val log = KotlinLogging.logger {}

class SagaManager(eventSubscriber: EventSubscriber, sagaRepository: SagaRepository, sagas: Set<Saga<*>>) {
    private val eventHandlers = sagas
            .map { it.getConfiguration() }
            .flatMap { saga ->
                saga.onEvents
                        .map { onEvent ->
                            onEvent.eventClass to { e: EventWrapper<Event<*>> ->
                                sagaRepository.getSagaState(onEvent.correlationId(e), AnnotationUtil.getSerializationId(saga.sagaClass), saga.stateClass)
                                        ?.let {
                                            onEvent.handler.invoke(e, Saga.SagaContext(it))
                                        }
                                        ?.let {
                                            if (it.newState != null || it.commands.isNotEmpty() || it.timeouts.isNotEmpty())
                                                SagaRepository.SagaUpsert.SagaUpdate(
                                                        correlationId = onEvent.correlationId(e),
                                                        serializationId = AnnotationUtil.getSerializationId(saga.sagaClass),
                                                        newState = it.newState,
                                                        commands = it.commands,
                                                        timeouts = it.timeouts.toSet()
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
                                                if (it.newState != null)
                                                    SagaRepository.SagaUpsert.SagaInsert(
                                                            correlationId = saga.initializer.correlationId(e),
                                                            serializationId = AnnotationUtil.getSerializationId(saga.sagaClass),
                                                            newState = it.newState!!,
                                                            commands = it.commands
                                                    )
                                                else
                                                    null
                                            }
                                }
                        )
            }
            .groupBy { it.first }

    private val timeoutHandlers = sagas
            .map { it.getConfiguration() }
            .flatMap { saga ->
                saga.onTimeout.map {
                    TimeoutHandlerId(
                            sagaSerializationId = AnnotationUtil.getSerializationId(saga.sagaClass),
                            timeoutId = it.timeoutId
                    ) to { correlationId: UUID ->
                        with(it.handler(Saga.SagaContext(sagaRepository.getSagaState(
                                correlationId = correlationId,
                                serializationId = AnnotationUtil.getSerializationId(saga.sagaClass),
                                sagaStateClass = saga.stateClass)
                                ?: error("no saga found on serializationId ${AnnotationUtil.getSerializationId(saga.sagaClass)}, correlationId $correlationId"))))
                        {
                            sagaRepository.update(SagaRepository.SagaUpsert.SagaUpdate(
                                    newState = newState,
                                    correlationId = correlationId,
                                    timeouts = timeouts.toSet(),
                                    commands = commands,
                                    serializationId = AnnotationUtil.getSerializationId(saga.sagaClass))
                            )
                        }
                    }
                }
            }
            .toMap()

    fun onTimeoutReady(sagaSerializationId: String, sagaCorrelationId: UUID, timeoutId: String) {
        val id = TimeoutHandlerId(sagaSerializationId, timeoutId)
        (timeoutHandlers[id] ?: error("no timeout handler found for id $id"))
                .invoke(sagaCorrelationId)
    }

    init {
        eventSubscriber.addSubscriber(
                consumerName = "SagaManager",
                fromEvent = sagaRepository.getCurrentHwm(),
                onEvent = { event ->
                    eventHandlers[event.event::class]
                            ?.mapNotNull { it.second.invoke(event) }
                            ?.toSet()
                            ?.apply {
                                sagaRepository.update(event.eventNumber, this)
                            }
                }
        )
    }

    data class TimeoutHandlerId(val sagaSerializationId: String, val timeoutId: String)
}

