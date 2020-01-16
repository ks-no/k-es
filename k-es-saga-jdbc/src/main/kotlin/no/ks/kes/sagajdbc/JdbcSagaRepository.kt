package no.ks.kes.sagajdbc

import java.util.*

class JdbcSagaRepository : SagaRepository{
    override fun get(correlationId: UUID, sagaSerializationId: String): ByteArray? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun save(correlationId: UUID, sagaSerializationId: String, data: ByteArray) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getCurrentHwm(): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun updateHwm(eventNumber: Long) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}