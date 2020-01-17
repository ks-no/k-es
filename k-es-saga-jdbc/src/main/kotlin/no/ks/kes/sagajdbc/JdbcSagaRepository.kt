package no.ks.kes.sagajdbc

import no.ks.kes.lib.SagaRepository
import java.util.*

class JdbcSagaRepository : SagaRepository {
    override fun get(correlationId: UUID, sagaSerializationId: String): ByteArray? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getCurrentHwm(): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun update(hwm: Long, states: Set<SagaRepository.NewSagaState>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}