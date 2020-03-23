package no.ks.kes.jdbc.saga

import mu.KotlinLogging
import no.ks.kes.jdbc.CmdTable
import no.ks.kes.jdbc.SagaTable
import no.ks.kes.jdbc.TimeoutTable
import no.ks.kes.jdbc.hwm.SqlServerHwmTrackerRepository
import no.ks.kes.lib.*
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*
import javax.sql.DataSource
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

class SqlServerSagaRepository(
        dataSource: DataSource,
        private val sagaStateSerdes: SagaStateSerdes<String>,
        private val cmdSerdes: CmdSerdes<String>
) : SagaRepository {
    private val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)

    override val hwmTracker = SqlServerHwmTrackerRepository(template)

    override fun transactionally(runnable: () -> Unit) {
        TransactionTemplate(transactionManager).execute {
            try {
                runnable.invoke()
            } catch (e: Exception) {
                log.error("An error was encountered while retrieving and executing saga-timeouts, transaction will be rolled back", e)
                throw e
            }
        }
    }

    override fun getReadyTimeouts(): SagaRepository.Timeout? {
        return template.query("""
            SELECT TOP 1 ${TimeoutTable.sagaSerializationId}, ${TimeoutTable.sagaCorrelationId}, ${TimeoutTable.timeoutId}             
            FROM $TimeoutTable 
            WITH (XLOCK)
            WHERE ${TimeoutTable.error} = 0
            AND ${TimeoutTable.timeout}  < CURRENT_TIMESTAMP 
        """) { r, _ ->
            SagaRepository.Timeout(
                    sagaSerializationId = r.getString(TimeoutTable.sagaSerializationId),
                    sagaCorrelationId = UUID.fromString(r.getString(TimeoutTable.sagaCorrelationId)),
                    timeoutId = r.getString(TimeoutTable.timeoutId)
            )
        }.singleOrNull()
    }

    override fun deleteTimeout(sagaSerializationId: String, sagaCorrelationId: UUID, timeoutId: String) {
        template.update(
                """DELETE FROM $TimeoutTable 
                   WHERE ${TimeoutTable.sagaCorrelationId} = :${TimeoutTable.sagaCorrelationId}
                   AND ${TimeoutTable.sagaSerializationId} = :${TimeoutTable.sagaSerializationId}
                   AND ${TimeoutTable.timeoutId} = :${TimeoutTable.timeoutId}
                """,
                mutableMapOf(
                        TimeoutTable.sagaSerializationId to sagaSerializationId,
                        TimeoutTable.sagaCorrelationId to sagaCorrelationId,
                        TimeoutTable.timeoutId to timeoutId
                )
        )
    }

    override fun <T : Any> getSagaState(correlationId: UUID, serializationId: String, sagaStateClass: KClass<T>): T? {
        return template.query(
                """
                    SELECT ${SagaTable.data}                                                            
                    FROM $SagaTable
                    WITH (XLOCK)
                    WHERE ${SagaTable.correlationId} = :${SagaTable.correlationId} 
                    AND ${SagaTable.serializationId} = :${SagaTable.serializationId}""",
                mutableMapOf(
                        SagaTable.correlationId to correlationId,
                        SagaTable.serializationId to serializationId
                )
        ) { r, _ -> r.getString(SagaTable.data) }
                .singleOrNull()
                ?.let {
                    sagaStateSerdes.deserialize(it, sagaStateClass)
                }
    }

    override fun update(states: Set<SagaRepository.SagaUpsert>) {
        log.info { "updating sagas: $states" }

        template.batchUpdate(
                "INSERT INTO $SagaTable (${SagaTable.correlationId}, ${SagaTable.serializationId}, ${SagaTable.data}) VALUES (:${SagaTable.correlationId}, :${SagaTable.serializationId}, :${SagaTable.data})",
                states.filterIsInstance<SagaRepository.SagaUpsert.SagaInsert>()
                        .map {
                            mutableMapOf(
                                    SagaTable.correlationId to it.correlationId,
                                    SagaTable.serializationId to it.serializationId,
                                    SagaTable.data to sagaStateSerdes.serialize(it.newState))
                        }
                        .toTypedArray()
        )

        template.batchUpdate(
                "INSERT INTO $TimeoutTable (${TimeoutTable.sagaCorrelationId}, ${TimeoutTable.sagaSerializationId}, ${TimeoutTable.timeoutId}, ${TimeoutTable.timeout}, ${TimeoutTable.error}) VALUES (:${TimeoutTable.sagaCorrelationId}, :${TimeoutTable.sagaSerializationId}, :${TimeoutTable.timeoutId}, :${TimeoutTable.timeout}, 0)",
                states.filterIsInstance<SagaRepository.SagaUpsert.SagaUpdate>()
                        .flatMap { saga ->
                            saga.timeouts.map {
                                mutableMapOf(
                                        TimeoutTable.sagaCorrelationId to saga.correlationId,
                                        TimeoutTable.sagaSerializationId to saga.serializationId,
                                        TimeoutTable.timeoutId to it.timeoutId,
                                        TimeoutTable.timeout to OffsetDateTime.ofInstant(it.triggerAt, ZoneOffset.UTC)
                                )
                            }
                        }
                        .toTypedArray()
        )

        template.batchUpdate(
                "UPDATE $SagaTable SET ${SagaTable.data} = :${SagaTable.data} WHERE ${SagaTable.correlationId} = :${SagaTable.correlationId} AND ${SagaTable.serializationId} = :${SagaTable.serializationId}",
                states.filterIsInstance<SagaRepository.SagaUpsert.SagaUpdate>()
                        .filter { it.newState != null }
                        .map {
                            mutableMapOf(
                                    SagaTable.correlationId to it.correlationId,
                                    SagaTable.serializationId to it.serializationId,
                                    SagaTable.data to sagaStateSerdes.serialize(it.newState!!))
                        }
                        .toTypedArray()
        )

        template.batchUpdate(
                """ 
                        INSERT INTO $CmdTable (${CmdTable.serializationId}, ${CmdTable.aggregateId}, ${CmdTable.retries}, ${CmdTable.nextExecution}, ${CmdTable.error}, ${CmdTable.data}) 
                        VALUES (:${CmdTable.serializationId}, :${CmdTable.aggregateId}, 0, :${CmdTable.nextExecution}, 0, :${CmdTable.data})                        
                        """,
                states.flatMap { it.commands }.map {
                    mutableMapOf(
                            CmdTable.serializationId to AnnotationUtil.getSerializationId(it::class),
                            CmdTable.aggregateId to it.aggregateId,
                            CmdTable.nextExecution to OffsetDateTime.now(ZoneOffset.UTC),
                            CmdTable.data to cmdSerdes.serialize(it))
                }
                        .toTypedArray())
    }
}