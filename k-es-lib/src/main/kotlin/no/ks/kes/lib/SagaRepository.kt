package no.ks.kes.lib

import java.util.*
import kotlin.reflect.KClass


interface SagaRepository: TransactionalRepository, HighWaterMarkedRepository {
    fun <T : Any> getSagaState(correlationId: UUID, serializationId: String, sagaStateClass: KClass<T>): T?
    fun update(states: Set<SagaUpsert>)
    fun getReadyTimeouts(): Timeout?
    fun deleteTimeout(timeout: Timeout)

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