package no.ks.kes.sagajdbc

import mu.KotlinLogging
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdHandler
import no.ks.kes.lib.CmdSerdes
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.*
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

class JdbcCommandQueue(dataSource: DataSource, private val cmdSerdes: CmdSerdes<String>, cmdHandlers: List<CmdHandler<*>>) {
    private val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)
    private val handledCmds = cmdHandlers.flatMap { handler -> handler.handledCmds().map { it to handler } }.toMap()

    fun poll() {
        TransactionTemplate(transactionManager).execute {
            val incomingCmd = nextCmd()

            incomingCmd?.let { wrapper ->
                val handler = handledCmds[wrapper.cmd::class] ?: error("no handler for cmd ${wrapper.cmd::class}")
                val result = try {
                    handler.handleAsync(wrapper.cmd, wrapper.retries)
                } catch (e: Exception) {
                    val errorId = UUID.randomUUID()
                    log.error("Error handling cmd ${wrapper.cmd::class.simpleName} (id: ${wrapper.id}), assigning errorId $errorId", e)
                    incrementAndSetError(wrapper.id, errorId)
                }

                when (result) {
                    is CmdHandler.AsyncResult.Success -> delete(wrapper.id)
                    is CmdHandler.AsyncResult.Fail -> delete(wrapper.id)
                    is CmdHandler.AsyncResult.Retry -> incrementAndSetNextExecution(wrapper.id, result.nextExecution)
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

    private fun incrementAndSetError(cmdId: Long, errorId: UUID) {
        template.update(
                "UPDATE $CmdTable SET ${CmdTable.error} = 1, ${CmdTable.errorId} = :${CmdTable.errorId}, ${CmdTable.retries} = ${CmdTable.retries} + 1 WHERE ${CmdTable.id} = :${CmdTable.id}",
                mutableMapOf(
                        CmdTable.id to cmdId,
                        CmdTable.errorId to errorId
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

    private fun nextCmd(): CmdWrapper<Cmd<*>>? = template.query(
            """ ;WITH cte AS
                    (
                        SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY ${CmdTable.aggregateId} ORDER BY ${CmdTable.id} DESC) AS rn
                        FROM cmd
                    )
                    SELECT TOP 1 aggregateId, retries, data
                    FROM cte WITH (UPDLOCK, NOWAIT)
                    WHERE rn = 1
                    AND ${CmdTable.error} = 0
                    AND ${CmdTable.nextExecution} > CURRENT_TIMESTAMP
                    ORDER BY NEWID()
                """
    ) { rs, _ ->
        CmdWrapper(
                id = rs.getLong(CmdTable.id),
                cmd = cmdSerdes.deserialize(rs.getString(CmdTable.data), rs.getString(CmdTable.serializationId)),
                retries = rs.getInt(CmdTable.retries)
        )
    }.singleOrNull()
}

data class CmdWrapper<T : Cmd<*>>(
        val id: Long,
        val cmd: T,
        val retries: Int
)