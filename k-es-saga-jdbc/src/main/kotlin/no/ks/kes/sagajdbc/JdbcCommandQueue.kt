package no.ks.kes.sagajdbc

import no.ks.kes.lib.*
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.*
import javax.sql.DataSource

class JdbcCommandQueue(dataSource: DataSource, private val cmdSerdes: CmdSerdes<String>, cmdHandlers: List<CmdHandler<*>>) {
    private val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)
    private val handledCmds = cmdHandlers.flatMap { handler -> handler.handledCmds().map { it to handler } }.toMap()

    fun doit() {
        TransactionTemplate(transactionManager).execute {
            val incomingCmd = poll()

            incomingCmd?.let { wrapper ->
                val handler = handledCmds[wrapper.cmd::class] ?: error("no handler for cmd ${wrapper.cmd::class}")
                when (val result = handler.handleAsync(wrapper.cmd, wrapper.retries)) {
                    is AsyncResult.Success -> delete(wrapper.id)
                    is AsyncResult.Fail -> delete(wrapper.id)
                    is AsyncResult.Retry -> incrementAndSetNextExecution(wrapper.id, result.nextExecution)
                }
            }
        }
    }

    private fun delete(cmdId: Long) {
        template.update(
                "UPDATE $CmdTable SET ${CmdTable.nextExecution} = :${CmdTable.nextExecution}, ${CmdTable.retries} = ${CmdTable.retries} + 1 WHERE ${CmdTable.id} = :${CmdTable.id}",
                mutableMapOf(
                        CmdTable.id to cmdId
                )
        )
    }

    private fun incrementAndSetNextExecution(cmdId: Long, nextExecution: Instant) {
        template.update(
                "UPDATE $CmdTable SET ${CmdTable.nextExecution} = :${CmdTable.nextExecution}, ${CmdTable.retries} = ${CmdTable.retries} + 1 WHERE ${CmdTable.id} = :${CmdTable.id}",
                mutableMapOf(
                        CmdTable.id to cmdId,
                        CmdTable.nextExecution to nextExecution
                )
        )
    }

    private fun poll(): CmdWrapper<Cmd<*>>? {
        val incomingCmd = template.query(
                """ ;WITH cte AS
                        (
                            SELECT *,
                            ROW_NUMBER() OVER (PARTITION BY ${CmdTable.aggregateId} ORDER BY ${CmdTable.id} DESC) AS rn
                            FROM cmd
                            WHERE ${CmdTable.failed} = 0
                            AND ${CmdTable.nextExecution} > CURRENT_TIMESTAMP
                        )
                        SELECT TOP 1 *
                        FROM cte WITH (UPDLOCK, NOWAIT)
                        WHERE rn = 1
                        ORDER BY NEWID()
                    """
        ) { rs, _ ->
            CmdWrapper(
                    id = rs.getLong(CmdTable.id),
                    cmd = cmdSerdes.deserialize(rs.getString(CmdTable.data), rs.getString(CmdTable.serializationId)),
                    aggregateId = UUID.fromString(rs.getString(CmdTable.aggregateId)),
                    retries = rs.getInt(CmdTable.retries),
                    nextExecution = rs.getTimestamp(CmdTable.nextExecution)?.toInstant(),
                    failed = rs.getBoolean(CmdTable.failed)
            )
        }.singleOrNull()
        return incomingCmd
    }
}

data class CmdWrapper<T : Cmd<*>>(
        val id: Long,
        val cmd: T,
        val aggregateId: UUID,
        val retries: Int,
        val nextExecution: Instant?,
        val failed: Boolean
)