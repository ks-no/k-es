package no.ks.kes.jdbc.saga

import no.ks.kes.jdbc.CmdTable
import no.ks.kes.lib.*
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*
import javax.sql.DataSource

class SqlServerCommandQueue(dataSource: DataSource, private val cmdSerdes: CmdSerdes<String>, cmdHandlers: Set<CmdHandler<*>>) : CommandQueue(cmdHandlers) {
    private val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)

    override fun delete(cmdId: Long) {
        template.update(
                "DELETE FROM $CmdTable WHERE ${CmdTable.id} = :${CmdTable.id}",
                mutableMapOf(
                        CmdTable.id to cmdId
                )
        )
    }

    override fun incrementAndSetError(cmdId: Long, errorId: UUID) {
        template.update(
                "UPDATE $CmdTable SET ${CmdTable.error} = 1, ${CmdTable.errorId} = :${CmdTable.errorId}, ${CmdTable.retries} = ${CmdTable.retries} + 1 WHERE ${CmdTable.id} = :${CmdTable.id}",
                mutableMapOf(
                        CmdTable.id to cmdId,
                        CmdTable.errorId to errorId
                )
        )
    }

    override fun incrementAndSetNextExecution(cmdId: Long, nextExecution: Instant) {
        template.update(
                "UPDATE $CmdTable SET ${CmdTable.nextExecution} = :${CmdTable.nextExecution}, ${CmdTable.retries} = ${CmdTable.retries} + 1 WHERE ${CmdTable.id} = :${CmdTable.id}",
                mutableMapOf(
                        CmdTable.id to cmdId,
                        CmdTable.nextExecution to OffsetDateTime.ofInstant(nextExecution, ZoneOffset.UTC)
                )
        )
    }

    override fun nextCmd(): CmdWrapper<Cmd<*>>? =
            template.query(
                    """ ;WITH cte AS
                    (
                        SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY ${CmdTable.aggregateId} ORDER BY ${CmdTable.id}) AS rn
                        FROM cmd
                    )
                    SELECT TOP 1 id, serializationId, aggregateId, retries, data
                    FROM cte
                    WITH (XLOCK)
                    WHERE rn = 1
                    AND ${CmdTable.error} = 0
                    AND ${CmdTable.nextExecution} < CURRENT_TIMESTAMP
                    ORDER BY NEWID()
                """
            ) { rs, _ ->
                CmdWrapper(
                        id = rs.getLong(CmdTable.id),
                        cmd = cmdSerdes.deserialize(rs.getString(CmdTable.data), rs.getString(CmdTable.serializationId)),
                        retries = rs.getInt(CmdTable.retries)
                )
            }.singleOrNull()

    override fun transactionally(runnable: () -> Unit) {
        TransactionTemplate(transactionManager).execute {
            runnable.invoke()
        }
    }
}