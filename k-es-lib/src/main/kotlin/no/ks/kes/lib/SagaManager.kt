package no.ks.kes.lib

import java.util.*
import kotlin.reflect.KClass


class SagaManager(sagas: Set<Saga<*>>) {
    private val eventHandlers = sagas
            .map { it.getConfiguration() }
            .flatMap { saga ->
                saga.onEvents
                        .map { onEvent ->
                            onEvent.eventClass to { e: EventWrapper<Event<*>>, r: (SagaId, KClass<Any>) -> Any? ->
                                r.invoke(SagaId(AnnotationUtil.getSerializationId(saga.sagaClass), onEvent.correlationId(e)), saga.stateClass)
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
                                                ) as SagaRepository.SagaUpsert
                                            else
                                                null
                                        }
                            }
                        }
                        .plus(
                                saga.initializer.eventClass to { e: EventWrapper<Event<*>>, _ ->
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
                    ) to { correlationId: UUID, r: (SagaId, KClass<Any>) -> Any? ->
                        with(it.handler(Saga.SagaContext(r.invoke(
                                SagaId(AnnotationUtil.getSerializationId(saga.sagaClass), correlationId),
                                saga.stateClass)
                                ?: error("no saga found on serializationId ${AnnotationUtil.getSerializationId(saga.sagaClass)}, correlationId $correlationId"))))
                        {
                            SagaRepository.SagaUpsert.SagaUpdate(
                                    newState = newState,
                                    correlationId = correlationId,
                                    timeouts = timeouts.toSet(),
                                    commands = commands,
                                    serializationId = AnnotationUtil.getSerializationId(saga.sagaClass)
                            )
                        }
                    }
                }
            }
            .toMap()

    fun onTimeout(sagaSerializationId: String, sagaCorrelationId: UUID, timeoutId: String, sagaStateRetriever: (SagaId, KClass<Any>) -> Any?): SagaRepository.SagaUpsert? {
        val id = TimeoutHandlerId(sagaSerializationId, timeoutId)
        return (timeoutHandlers[id] ?: error("no timeout handler found for id $id"))
                .invoke(sagaCorrelationId, sagaStateRetriever)
    }

    fun onEvent(event: EventWrapper<Event<*>>, sagaStateRetriever: (SagaId, KClass<Any>) -> Any?): Set<SagaRepository.SagaUpsert> =
            eventHandlers[event.event::class]
                    ?.mapNotNull { it.second.invoke(event, sagaStateRetriever) }
                    ?.toSet() ?: emptySet()

    data class TimeoutHandlerId(val sagaSerializationId: String, val timeoutId: String)
    data class SagaId(val serializationId: String, val correlationId: UUID)
}

