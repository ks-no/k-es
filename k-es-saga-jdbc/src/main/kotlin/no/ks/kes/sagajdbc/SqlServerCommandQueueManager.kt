package no.ks.kes.sagajdbc

import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdHandler
import no.ks.kes.lib.CmdSerdes
import java.time.Instant
import java.util.*
import javax.sql.DataSource

class SqlServerCommandQueueManager(dataSource: DataSource, private val cmdSerdes: CmdSerdes<String>, cmdHandlers: Set<CmdHandler<*>>) : JdbcCommandQueue(dataSource, cmdHandlers) {

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
                        CmdTable.nextExecution to nextExecution
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
}