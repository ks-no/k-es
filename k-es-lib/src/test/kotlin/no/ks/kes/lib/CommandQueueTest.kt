package no.ks.kes.lib

import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import no.ks.kes.lib.testdomain.Employee
import java.time.Instant
import java.util.*

@ExperimentalStdlibApi
class CommandQueueTest : StringSpec() {
    class TestQueue(cmdHandler: CmdHandler<*>) : CommandQueue(setOf(cmdHandler)) {
        override fun delete(cmdId: Long) {}
        override fun incrementAndSetError(cmdId: Long, errorId: UUID) {}
        override fun incrementAndSetNextExecution(cmdId: Long, nextExecution: Instant) {}
        override fun nextCmd(): CmdWrapper<Cmd<*>>? = null
        override fun transactionally(runnable: () -> Unit) {
            runnable.invoke()
        }
    }

    init {
        "test that a command which is executed successfully is removed from the queue"{
            data class SomeCmd(override val aggregateId: UUID) : Cmd<Employee>

            val queue = spyk(
                    objToCopy = TestQueue(object : CmdHandler<Employee>(
                            mockk<AggregateRepository>().apply {
                                every { read(any(), any<Employee>()) } returns Employee()
                            }) {
                        override fun initAggregate(): Employee = Employee()

                        init {
                            on<SomeCmd> {
                                Result.Succeed()
                            }
                        }
                    }),
                    recordPrivateCalls = true)

            every { queue["nextCmd"]() } returns CmdWrapper(cmd = SomeCmd(UUID.randomUUID()), id = 1L, retries = 0)

            queue.poll()

            verify { queue["delete"](1L) }
        }

        "test that a command which is fails permanently is removed from the queue"{
            data class SomeCmd(override val aggregateId: UUID) : Cmd<Employee>

            val queue = spyk(
                    objToCopy = TestQueue(object : CmdHandler<Employee>(
                            mockk<AggregateRepository>().apply {
                                every { read(any(), any<Employee>()) } returns Employee()
                            }) {
                        override fun initAggregate(): Employee = Employee()

                        init {
                            on<SomeCmd> {
                                Result.Fail(IllegalStateException("something went wrong"))
                            }
                        }
                    }),
                    recordPrivateCalls = true)
            every { queue["nextCmd"]() } returns CmdWrapper(cmd = SomeCmd(UUID.randomUUID()), id = 1L, retries = 0)

            queue.poll()

            verify { queue["delete"](1L) }
        }

        "test that a command which throws an uncaught exception is marked as in error"{
            data class SomeCmd(override val aggregateId: UUID) : Cmd<Employee>

            val queue = spyk(
                    objToCopy = TestQueue(object : CmdHandler<Employee>(
                            mockk<AggregateRepository>().apply {
                                every { read(any(), any<Employee>()) } returns Employee()
                            }) {
                        override fun initAggregate(): Employee = Employee()

                        init {
                            on<SomeCmd> {
                                error("something went wrong")
                            }
                        }
                    }),
                    recordPrivateCalls = true)
            every { queue["nextCmd"]() } returns CmdWrapper(cmd = SomeCmd(UUID.randomUUID()), id = 1L, retries = 0)

            queue.poll()

            verify { queue["incrementAndSetError"](1L, any<UUID>())}
        }


        "test that a command fails with retry is designated with a new execution if the retry strategy allows"{
            data class SomeCmd(override val aggregateId: UUID) : Cmd<Employee>

            val queue = spyk(
                    objToCopy = TestQueue(object : CmdHandler<Employee>(
                            mockk<AggregateRepository>().apply {
                                every { read(any(), any<Employee>()) } returns Employee()
                            }) {
                        override fun initAggregate(): Employee = Employee()

                        init {
                            on<SomeCmd> {
                                Result.RetryOrFail(IllegalStateException("something went wrong"), RetryStrategies.DEFAULT)
                            }
                        }
                    }),
                    recordPrivateCalls = true)
            every { queue["nextCmd"]() } returns CmdWrapper(cmd = SomeCmd(UUID.randomUUID()), id = 1L, retries = 0)

            queue.poll()

            verify { queue["incrementAndSetNextExecution"](1L, any<Instant>())}
        }
    }
}