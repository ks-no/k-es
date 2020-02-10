package no.ks.kes.lib

import mu.KotlinLogging
import java.util.*
import kotlin.concurrent.schedule
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

class Sagas internal constructor(sagas: Set<Saga<*>>, stateRetriever: (SagaId, KClass<Any>) -> Any?) {

    fun onEvent(event: EventWrapper<Event<*>>): Set<SagaRepository.SagaUpsert> {
        return eventHandlers[event.event::class]
                ?.mapNotNull {
                    it.second.invoke(event)
                }
                ?.toSet() ?: emptySet()
    }

    fun onTimeout(sagaSerializationId: String, sagaCorrelationId: UUID, timeoutId: String): SagaRepository.SagaUpsert.SagaUpdate {
        val id = TimeoutHandlerId(sagaSerializationId, timeoutId)
        return (timeoutHandlers[id] ?: error("no timeout handler found for id $id"))
                .invoke(sagaCorrelationId)
    }

    private val eventHandlers = sagas
            .map { it.getConfiguration() }
            .flatMap { saga ->
                saga.onEvents
                        .map { onEvent ->
                            onEvent.eventClass to { e: EventWrapper<Event<*>> ->
                                stateRetriever.invoke(SagaId(AnnotationUtil.getSerializationId(saga.sagaClass), onEvent.correlationId(e)), saga.stateClass)
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
                        with(it.handler(Saga.SagaContext(stateRetriever.invoke(
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

    data class TimeoutHandlerId(val sagaSerializationId: String, val timeoutId: String)
    data class SagaId(val serializationId: String, val correlationId: UUID)

    companion object {
        fun initialize(eventSubscriber: EventSubscriber, sagaRepository: SagaRepository, sagas: Set<Saga<*>>, commandQueue: CommandQueue, pollInterval: Long = 5000) {
            val sagaManager = Sagas(sagas) { id, stateClass ->
                sagaRepository.getSagaState(
                        correlationId = id.correlationId,
                        serializationId = id.serializationId,
                        sagaStateClass = stateClass
                )
            }
            eventSubscriber.addSubscriber(
                    consumerName = "SagaManager",
                    fromEvent = sagaRepository.currentHwm(),
                    onEvent = { event ->
                        sagaRepository.transactionally {
                            try {
                                sagaRepository.update(
                                        hwm = event.eventNumber,
                                        states = sagaManager.onEvent(event)
                                )
                            } catch (e: Exception) {
                                log.error("An error was encountered while handling incoming event ${event.event::class.simpleName} with sequence number ${event.eventNumber}", e)
                                throw e
                            }
                        }
                    }
            )

            Timer("PollingTimeouts", false).schedule(0, pollInterval) {
                sagaRepository.transactionally {
                    sagaRepository.getReadyTimeouts()
                            ?.also {
                                log.debug { "polled for timeouts, found timeout with sagaSerializationId: \"${it.sagaSerializationId}\", sagaCorrelationId: \"${it.sagaCorrelationId}\", timeoutId: \"${it.timeoutId}\"" }
                            }
                            ?.apply {
                                sagaManager.onTimeout(sagaSerializationId, sagaCorrelationId, timeoutId)
                                        .apply { sagaRepository.update(this) }
                                sagaRepository.deleteTimeout(sagaSerializationId, sagaCorrelationId, timeoutId)
                            } ?: log.debug { "polled for timeouts, found none" }
                }
            }

            Timer("PollingCommandQueue", false).schedule(0, pollInterval) {
                commandQueue.poll()
            }
        }
    }
}

