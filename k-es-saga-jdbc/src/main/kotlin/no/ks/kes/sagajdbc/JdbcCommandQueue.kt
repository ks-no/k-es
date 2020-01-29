package no.ks.kes.sagajdbc

import mu.KotlinLogging
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdHandler
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.*
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

abstract class JdbcCommandQueue(
        dataSource: DataSource,
        cmdHandlers: Set<CmdHandler<*>>,
        private val shouldProcessCmds: () -> Boolean = { true }) {

    protected val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)
    private val handledCmds = cmdHandlers.flatMap { handler -> handler.handledCmds().map { it to handler } }.toMap()
    private var currentShouldProcessCmds = true

    fun poll() {
        currentShouldProcessCmds = shouldProcessCmds.invoke().also {
            logShouldProcess(it)
        }

        if (currentShouldProcessCmds) {
            TransactionTemplate(transactionManager).execute {
                try {
                    val incomingCmd = nextCmd()

                    if (incomingCmd == null)
                        log.info { "polled for cmds, found none" }
                    else
                        log.info { "polled for cmds, found cmd with id ${incomingCmd.id}" }

                    incomingCmd?.let { wrapper ->
                        val handler = handledCmds[wrapper.cmd::class]
                                ?: error("no handler for cmd ${wrapper.cmd::class}")
                        val result = try {
                            handler.handleAsync(wrapper.cmd, wrapper.retries)
                        } catch (e: Exception) {
                            val errorId = UUID.randomUUID()
                            log.error("Error handling cmd ${wrapper.cmd::class.simpleName} (id: ${wrapper.id}), assigning errorId $errorId", e)
                            incrementAndSetError(wrapper.id, errorId)
                            throw e
                        }

                        when (result) {
                            is CmdHandler.AsyncResult.Success -> delete(wrapper.id)
                            is CmdHandler.AsyncResult.Fail -> delete(wrapper.id)
                            is CmdHandler.AsyncResult.Retry -> incrementAndSetNextExecution(wrapper.id, result.nextExecution)
                        }
                    }
                } catch (e: Exception) {
                    log.error("An exception was encountered while executing cmd, transaction will roll back", e)
                }
            }
        }
    }

    private fun logShouldProcess(shouldProcess: Boolean) {
        if (currentShouldProcessCmds != shouldProcessCmds.invoke())
            if (shouldProcess)
                log.info("The \"Should process cmds\" indicator has changed to $shouldProcess. The queue will now retrieve and process new commands")
            else
                log.info("The \"Should process cmds\" indicator has changed to $shouldProcess. The queue will no longer retrieve and process new commands")
    }

    protected abstract fun delete(cmdId: Long)
    protected abstract fun incrementAndSetError(cmdId: Long, errorId: UUID)
    protected abstract fun incrementAndSetNextExecution(cmdId: Long, nextExecution: Instant)
    protected abstract fun nextCmd(): CmdWrapper<Cmd<*>>?

}

data class CmdWrapper<T : Cmd<*>>(
        val id: Long,
        val cmd: T,
        val retries: Int
)