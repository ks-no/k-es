package no.ks.kes.lib

import mu.KotlinLogging
import java.time.Instant
import java.util.*
import kotlin.system.exitProcess

private val log = KotlinLogging.logger {}

abstract class CommandQueue(
        cmdHandlers: Set<CmdHandler<*>>,
        private val shouldProcessCmds: () -> Boolean = { true }) {

    private val handledCmds = cmdHandlers.flatMap { handler -> handler.handledCmds().map { it to handler } }.toMap()
    private var currentShouldProcessCmds = true

    fun poll() {
        currentShouldProcessCmds = shouldProcessCmds.invoke().also {
            logShouldProcess(it)
        }

        if (currentShouldProcessCmds) {
            transactionally {
                try {
                    val incomingCmd = nextCmd()

                    if (incomingCmd == null)
                        log.debug { "polled for cmds, found none" }
                    else
                        log.debug { "polled for cmds, found cmd with id ${incomingCmd.id}" }

                    incomingCmd?.let { wrapper ->
                        val handler = handledCmds[wrapper.cmd::class]
                                ?: error("no handler for cmd ${wrapper.cmd::class}")
                        val result = try {
                            handler.handleAsync(wrapper.cmd, wrapper.retries)
                        } catch (e: Exception) {
                            log.error("Internal error handling cmd ${wrapper.cmd::class.simpleName} (id: ${wrapper.id}), will quit process", e)
                            exitProcess(1)
                        }

                        when (result) {
                            is CmdHandler.AsyncResult.Success -> delete(wrapper.id)
                            is CmdHandler.AsyncResult.Fail -> delete(wrapper.id)
                            is CmdHandler.AsyncResult.Error -> {
                                val errorId = UUID.randomUUID()
                                log.error("Error handling cmd ${wrapper.cmd::class.simpleName} (id: ${wrapper.id}), assigning errorId $errorId", result.exception)
                                incrementAndSetError(wrapper.id, errorId)
                            }
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
    protected abstract fun transactionally(runnable: () -> Unit)
}

data class CmdWrapper<T : Cmd<*>>(
        val id: Long,
        val cmd: T,
        val retries: Int
)