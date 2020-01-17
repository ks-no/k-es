package no.ks.kes.lib

import java.util.*

interface SagaRepository {
    fun get(correlationId: UUID, sagaSerializationId: String): ByteArray?
    fun getCurrentHwm(): Long
    fun update(hwm: Long, states: Set<NewSagaState>)

    data class NewSagaState(val correlationId: UUID, val sagaSerializationId: String, val data: ByteArray?, val commands: List<Cmd<*>>)

}