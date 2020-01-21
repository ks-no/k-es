package no.ks.kes.sagajdbc

import no.ks.kes.lib.AnnotationUtil
import no.ks.kes.lib.CmdSerdes
import no.ks.kes.lib.SagaRepository
import no.ks.kes.lib.SagaStateSerdes
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import javax.sql.DataSource
import kotlin.reflect.KClass



class JdbcSagaRepository(
        dataSource: DataSource,
        private val sagaStateSerdes: SagaStateSerdes<String>,
        private val cmdSerdes: CmdSerdes<String>
) : SagaRepository {
    private val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)

    override fun <T : Any> getSagaState(correlationId: UUID, serializationId: String, sagaStateClass: KClass<T>): T? {
        return template.queryForObject(
                "SELECT ${SagaTable.data} FROM $SagaTable WHERE ${SagaTable.correlationId} = :${SagaTable.correlationId} AND ${SagaTable.serializationId} = :${SagaTable.serializationId}",
                mutableMapOf(
                        SagaTable.correlationId to correlationId,
                        SagaTable.serializationId to serializationId
                ),
                String::class.java
        )
                ?.let {
                    sagaStateSerdes.deserialize(it, sagaStateClass)
                }
    }

    override fun getCurrentHwm(): Long =
            template.queryForObject(
                    "SELECT ${HwmTable.hwm} FROM ${HwmTable}",
                    mutableMapOf<String, Any>(),
                    Long::class.java
            ) ?: error("No hwm found in ${SagaTable}_HWM")

    override fun update(hwm: Long, states: Set<SagaRepository.SagaUpsert>) {
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
                    "INSERT INTO $CmdTable (${CmdTable.serializationId}, ${CmdTable.data}) VALUES (:${CmdTable.serializationId}, :${CmdTable.data})",
                    states.flatMap { it.commands }.map {
                        mutableMapOf(
                                CmdTable.serializationId to AnnotationUtil.getSerializationId(it::class),
                                CmdTable.data to cmdSerdes.serialize(it))
                    }
                            .toTypedArray())

            template.update("UPDATE $HwmTable SET ${HwmTable.hwm} = :${HwmTable.hwm}",
                    mutableMapOf(HwmTable.hwm to hwm))
        }
    }

}