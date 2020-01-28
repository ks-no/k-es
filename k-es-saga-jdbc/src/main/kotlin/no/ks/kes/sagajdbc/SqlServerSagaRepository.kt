package no.ks.kes.sagajdbc

import mu.KotlinLogging
import no.ks.kes.lib.AnnotationUtil
import no.ks.kes.lib.CmdSerdes
import no.ks.kes.lib.SagaRepository
import no.ks.kes.lib.SagaStateSerdes
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.core.simple.SimpleJdbcCall
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*
import javax.sql.DataSource
import kotlin.reflect.KClass
import kotlin.system.exitProcess

private val log = KotlinLogging.logger {}
private const val SAGA_LOCK_TIMEOUT = 60


class SqlServerSagaRepository(
        dataSource: DataSource,
        private val sagaStateSerdes: SagaStateSerdes<String>,
        private val cmdSerdes: CmdSerdes<String>
) : SagaRepository {
    private val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)
    private val obtainApplicationLock: SimpleJdbcCall = SimpleJdbcCall(dataSource)
            .withFunctionName("sp_getapplock")

    override fun <T : Any> getSagaState(correlationId: UUID, serializationId: String, sagaStateClass: KClass<T>): T? {
        val lockRequestResponse = obtainApplicationLock.executeFunction(
                Int::class.java,
                mutableMapOf(
                        "LockOwner" to "Transaction",
                        "LockMode" to "Exclusive",
                        "Resource" to correlationId.toString() + serializationId,
                        "DbPrincipal" to "public",
                        "LockTimeout" to SAGA_LOCK_TIMEOUT
                ))

        if (lockRequestResponse != 0) {
            log.error { "Could not obtain lock on saga $correlationId-$serializationId: lock request response was $lockRequestResponse" }
            exitProcess(1)
        }

        return template.query(
                """
                    SELECT ${SagaTable.data}                    
                    FROM $SagaTable
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
        TransactionTemplate(transactionManager).executeWithoutResult {
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