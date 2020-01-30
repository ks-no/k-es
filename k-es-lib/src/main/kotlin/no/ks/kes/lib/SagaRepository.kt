package no.ks.kes.lib

import mu.KotlinLogging
import java.util.*
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

abstract class SagaRepository(private val eventSubscriber: EventSubscriber, private val sagaManager: SagaManager) {
    protected abstract fun <T : Any> getSagaState(correlationId: UUID, serializationId: String, sagaStateClass: KClass<T>): T?
    protected abstract fun getCurrentHwm(): Long
    protected abstract fun update(hwm: Long, states: Set<SagaUpsert>)
    protected abstract fun update(upsert: SagaUpsert.SagaUpdate)
    protected abstract fun transactionally(runnable: () -> Unit)
    protected abstract fun getReadyTimeouts(): Timeout?
    protected abstract fun deleteTimeout(sagaSerializationId: String, sagaCorrelationId: UUID, timeoutId: String)

    protected fun subscribeToEvents(fromEvent: Long){
        eventSubscriber.addSubscriber(
                consumerName = "SagaManager",
                fromEvent = fromEvent,
                onEvent = { event ->
                    transactionally {
                        try {
                            update(event.eventNumber, sagaManager.onEvent(event) { id, stateClass -> getSagaState(id.correlationId, id.serializationId, stateClass) })
                        } catch (e: Exception) {
                            log.error("An error was encountered while handling incoming event ${event.event::class.simpleName} with sequence number ${event.eventNumber}", e)
                            throw e
                        }
                    }
                }
        )
    }

    fun poll() {
        transactionally {
            try {
                getReadyTimeouts()
                        ?.also {
                            log.info { "polled for timeouts, found timeout with sagaSerializationId: \"${it.sagaSerializationId}\", sagaCorrelationId: \"${it.sagaCorrelationId}\", timeoutId: \"${it.timeoutId}\"" }
                        }
                        ?.apply {
                            sagaManager.onTimeout(sagaSerializationId, sagaCorrelationId, timeoutId) { id, stateClass -> getSagaState(id.correlationId, id.serializationId, stateClass) }
                                    ?.apply { update(this as SagaRepository.SagaUpsert.SagaUpdate) }
                            deleteTimeout(sagaSerializationId, sagaCorrelationId, timeoutId)
                        } ?: log.info { "polled for timeouts, found none" }
            } catch (e: Exception) {
                log.error("An error was encountered while retrieving and executing saga-timeouts, transaction will be rolled back", e)
                throw e
            }
        }
    }

    data class Timeout(val sagaCorrelationId: UUID, val sagaSerializationId: String, val timeoutId: String)
    sealed class SagaUpsert {

        abstract val commands: List<Cmd<*>>

        data class SagaUpdate(
                val correlationId: UUID,
                val serializationId: String,
                val newState: Any?,
                val timeouts: Set<Saga.Timeout>,
                override val commands: List<Cmd<*>>
        ) : SagaUpsert()

        data class SagaInsert(
                val correlationId: UUID,
                val serializationId: String,
                val newState: Any,
                override val commands: List<Cmd<*>>
        ) : SagaUpsert()
    }
}