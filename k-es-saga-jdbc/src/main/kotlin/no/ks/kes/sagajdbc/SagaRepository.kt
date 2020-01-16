package no.ks.kes.sagajdbc

import java.util.*

interface SagaRepository {
    fun get(correlationId: UUID, sagaSerializationId: String): ByteArray?
    fun save(correlationId: UUID, sagaSerializationId: String, data: ByteArray)
    fun getCurrentHwm(): Long
    fun updateHwm(eventNumber: Long)
    abstract fun update(sagaStates: Any)

}