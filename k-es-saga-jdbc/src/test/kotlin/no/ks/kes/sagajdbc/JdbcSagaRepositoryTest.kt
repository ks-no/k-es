package no.ks.kes.sagajdbc

import io.mockk.every
import io.mockk.mockk
import no.ks.kes.lib.SagaRepository
import no.ks.kes.lib.SagaStateSerdes
import org.junit.jupiter.api.Test
import org.zapodot.junit.db.annotations.EmbeddedDatabase
import org.zapodot.junit.db.annotations.EmbeddedDatabaseTest
import org.zapodot.junit.db.common.Engine
import java.util.*
import javax.sql.DataSource

@EmbeddedDatabaseTest(
        engine = Engine.H2,
        initialSqls = [
            "CREATE TABasdfaLE saga(serializationId VARCHAR(512), correlationId VARCHAR(512), data VARCHAR)",
            "CREATE TABLE hwm(hwm BIGINT)"
        ]
)
class JdbcSagaRepositoryTest{

    @Test
    internal fun name(@EmbeddedDatabase dataSource: DataSource) {
        data class SomeState(val aggregateId: UUID)

        val sagaStateSerdes = mockk<SagaStateSerdes<String>>().apply { every { serialize(any()) } returns "{\"aggregateId\": \"737aff2e-8cf2-4cbb-819c-f9e3f4d3b0f2\"}" }
        val repo = JdbcSagaRepository(dataSource, sagaStateSerdes, mockk())
        repo.update(1L, setOf(SagaRepository.SagaUpsert.SagaInsert(UUID.randomUUID(), "foo", SomeState(UUID.randomUUID()), emptyList())))
    }
}