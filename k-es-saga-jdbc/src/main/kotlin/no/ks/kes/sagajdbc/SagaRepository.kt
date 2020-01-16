package no.ks.kes.sagajdbc

import java.util.*
import javax.sql.DataSource

interface SagaRepository {
    fun get(correlationId: UUID, sagaSerializationId: String): ByteArray?

    fun save(correlationId: UUID, sagaSerializationId: String, data: ByteArray)

}