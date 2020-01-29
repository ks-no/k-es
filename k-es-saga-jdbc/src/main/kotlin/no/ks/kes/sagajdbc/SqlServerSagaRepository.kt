package no.ks.kes.sagajdbc

import mu.KotlinLogging
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
private const val SAGA_LOCK_TIMEOUT = 60000


class SqlServerSagaRepository(
        dataSource: DataSource,
        private val sagaStateSerdes: SagaStateSerdes<String>,
        private val cmdSerdes: CmdSerdes<String>,
        eventSubscriber: EventSubscriber,
        val sagaManager: SagaManager
) : SagaRepository {
    private val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)

    init {
        eventSubscriber.addSubscriber(
                consumerName = "SagaManager",
                fromEvent = getCurrentHwm(),
                onEvent = { event ->
                    TransactionTemplate(transactionManager).executeWithoutResult {
                        try {
                            update(event.eventNumber, sagaManager.onEvent(event) { id, stateClass -> getSagaState(id.correlationId, id.serializationId, stateClass) })
                        } catch (e: Exception) {
                            log.error("An error was encountered while handling incoming event ${event.event::class.simpleName} with sequence number ${event.eventNumber}", e)
                            throw e
                        }
                    }
                }
        )
    }

    fun poll() {
        TransactionTemplate(transactionManager).executeWithoutResult {
            try {
                getReadyTimeouts()
                        ?.also {
                            log.info { "polled for timeouts, found timeout with sagaSerializationId: \"${it.sagaSerializationId}\", sagaCorrelationId: \"${it.sagaCorrelationId}\", timeoutId: \"${it.timeoutId}\"" }
                        }
                        ?.apply {
                            sagaManager.onTimeout(sagaSerializationId, sagaCorrelationId, timeoutId) { id, stateClass -> getSagaState(id.correlationId, id.serializationId, stateClass) }
                                    ?.apply { update(this as SagaRepository.SagaUpsert.SagaUpdate) }
                            deleteTimeout(sagaSerializationId, sagaCorrelationId, timeoutId)
                        } ?: log.info { "polled for timeouts, found none" }
            } catch (e: Exception) {
                log.error("An error was encountered while retrieving and executing saga-timeouts, transaction will be rolled back", e)
                throw e
            }
        }
    }

    private fun getReadyTimeouts(): SagaRepository.Timeout? {
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

    private fun deleteTimeout(sagaSerializationId: String, sagaCorrelationId: UUID, timeoutId: String) {
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
        ) { r, i -> r.getString(SagaTable.data) }
                .singleOrNull()
                ?.let {
                    sagaStateSerdes.deserialize(it, sagaStateClass)
                }
    }

    override fun getCurrentHwm(): Long =
            template.queryForObject(
                    "SELECT ${HwmTable.sagaHwm} FROM $HwmTable",
                    mutableMapOf<String, Any>(),
                    Long::class.java
            ) ?: error("No hwm found in ${SagaTable}_HWM")


    override fun update(hwm: Long, states: Set<SagaRepository.SagaUpsert>) {
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

        template.update("UPDATE $HwmTable SET ${HwmTable.sagaHwm} = :${HwmTable.sagaHwm}",
                mutableMapOf(HwmTable.sagaHwm to hwm))

    }

    override fun update(upsert: SagaRepository.SagaUpsert.SagaUpdate) {
        upsert.newState?.apply {
            template.update(
                    "UPDATE $SagaTable SET ${SagaTable.data} = :${SagaTable.data} WHERE ${SagaTable.correlationId} = :${SagaTable.correlationId} AND ${SagaTable.serializationId} = :${SagaTable.serializationId}",
                    mutableMapOf(
                            SagaTable.correlationId to upsert.correlationId,
                            SagaTable.serializationId to upsert.serializationId,
                            SagaTable.data to sagaStateSerdes.serialize(this)))
        }

        if (upsert.commands.isNotEmpty())
            template.batchUpdate(
                    """ 
                        INSERT INTO $CmdTable (${CmdTable.serializationId}, ${CmdTable.aggregateId}, ${CmdTable.retries}, ${CmdTable.nextExecution}, ${CmdTable.error}, ${CmdTable.data}) 
                        VALUES (:${CmdTable.serializationId}, :${CmdTable.aggregateId}, 0, :${CmdTable.nextExecution}, 0, :${CmdTable.data})                        
                        """,
                    upsert.commands.map {
                        mutableMapOf(
                                CmdTable.serializationId to AnnotationUtil.getSerializationId(it::class),
                                CmdTable.aggregateId to it.aggregateId,
                                CmdTable.nextExecution to OffsetDateTime.now(ZoneOffset.UTC),
                                CmdTable.data to cmdSerdes.serialize(it))
                    }
                            .toTypedArray())
    }

}