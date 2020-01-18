package no.ks.kes.sagajdbc

import io.mockk.every
import io.mockk.mockk
import no.ks.kes.lib.SagaRepository
import no.ks.kes.lib.SagaStateSerdes
import no.ks.kes.serdes.jackson.JacksonSagaStateSerdes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.zapodot.junit.db.annotations.EmbeddedDatabase
import org.zapodot.junit.db.annotations.EmbeddedDatabaseTest
import org.zapodot.junit.db.common.Engine
import java.util.*
import javax.sql.DataSource

@EmbeddedDatabaseTest(
        engine = Engine.H2,
        initialSqls = [
            "CREATE TABLE saga(serializationId VARCHAR(512), correlationId VARCHAR(512), data VARCHAR)",
            "CREATE TABLE hwm(hwm BIGINT)"
        ]
)
class JdbcSagaRepositoryTest{
    data class SomeState(val aggregateId: UUID)

    @Test
    internal fun name(@EmbeddedDatabase dataSource: DataSource) {
        val repo = JdbcSagaRepository(dataSource, JacksonSagaStateSerdes(), mockk())
        val correlationId = UUID.randomUUID()
        val serializationId = "foo"
        val sagaState = SomeState(UUID.randomUUID())
        repo.update(1L, setOf(SagaRepository.SagaUpsert.SagaInsert(correlationId, serializationId, sagaState, emptyList())))
        repo.getSagaState(correlationId, serializationId, SomeState::class)!!
                .apply { assertEquals(sagaState, this )
        }

    }
}