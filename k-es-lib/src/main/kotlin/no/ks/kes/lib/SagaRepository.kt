package no.ks.kes.lib

import java.util.*
import kotlin.reflect.KClass


interface SagaRepository : TransactionalRepository, HighWaterMarkedRepository {
    fun <T : Any> getSagaState(correlationId: UUID, serializationId: String, sagaStateClass: KClass<T>): T?
    fun update(states: Set<Operation>)
    fun getReadyTimeouts(): Timeout?
    fun deleteTimeout(timeout: Timeout)

    data class Timeout(val sagaCorrelationId: UUID, val sagaSerializationId: String, val timeoutId: String)
    sealed class Operation {

        abstract val commands: List<Cmd<*>>

        data class SagaUpdate(
                val correlationId: UUID,
                val serializationId: String,
                val newState: Any?,
                val timeouts: Set<Saga.Timeout>,
                override val commands: List<Cmd<*>>
        ) : Operation()

        data class Insert(
                val correlationId: UUID,
                val serializationId: String,
                val newState: Any,
                override val commands: List<Cmd<*>>
        ) : Operation()
    }
}