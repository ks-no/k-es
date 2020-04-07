package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import java.time.Instant
import java.time.LocalDate
import java.util.*

internal class CmdHandlerTest : StringSpec() {
    data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    @SerializationId("some-id")
    data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>

    val someAggregateConfiguration = object : AggregateConfiguration<SomeAggregate>("some-aggregate") {
        init {
            init<SomeEvent> {
                SomeAggregate(stateInitialized = true)
            }

            apply<SomeEvent> {
                copy(stateUpdated = true)
            }
        }
    }

    init {
        "Test that multiple \"init\" handlers results in an exception being thrown" {
            data class HireCmd(override val aggregateId: UUID, val startDate: LocalDate) : Cmd<SomeAggregate>
            shouldThrow<IllegalStateException> {
                object : CmdHandler<SomeAggregate>(mockk(), mockk()) {
                    init {
                        init<HireCmd> {
                            Result.Succeed(SomeEvent(it.aggregateId, Instant.now()))
                        }

                        init<HireCmd> {
                            Result.Succeed(SomeEvent(it.aggregateId, Instant.now()))
                        }
                    }
                }
            }.apply {
                message shouldBe "There are multiple \"init\" configurations for the command HireCmd in the command handler null, only a single \"init\" handler is allowed for each command"
            }
        }

        "Test that multiple \"apply\" handlers results in an exception being thrown" {
            data class HireCmd(override val aggregateId: UUID, val startDate: LocalDate) : Cmd<SomeAggregate>
            shouldThrow<IllegalStateException> {
                object : CmdHandler<SomeAggregate>(mockk(), mockk()) {
                    init {
                        apply<HireCmd> {
                            Result.Succeed(SomeEvent(it.aggregateId, Instant.now()))
                        }

                        apply<HireCmd> {
                            Result.Succeed(SomeEvent(it.aggregateId, Instant.now()))
                        }
                    }
                }
            }.apply {
                message shouldBe "There are multiple \"apply\" configurations for the command HireCmd in the command handler null, only a single \"apply\" handler is allowed for each command"
            }
        }

        "Test that a command can result in an event being applied to an existing aggregate" {
            data class SomeCmd(override val aggregateId: UUID) : Cmd<SomeAggregate>

            val someCmd = SomeCmd(
                    aggregateId = UUID.randomUUID()
            )

            val repoMock = mockk<AggregateRepository>().apply {
                every { read(someCmd.aggregateId, someAggregateConfiguration) } returns
                        AggregateReadResult.ExistingAggregate(SomeAggregate(true), 0)
                every { append("some-aggregate", someCmd.aggregateId, ExpectedEventNumber.Exact(0), any()) } returns
                        Unit
            }

            object : CmdHandler<SomeAggregate>(repoMock, someAggregateConfiguration) {
                init {
                    apply<SomeCmd> {
                        Result.Succeed(SomeEvent(it.aggregateId, Instant.now()))
                    }
                }
            }.handle(someCmd).apply {
                stateInitialized shouldBe true
            }
        }

        "Test that a command \"Fail\" results in an exception being thrown" {
            data class SomeCmd(override val aggregateId: UUID) : Cmd<SomeAggregate>

            val someCmd = SomeCmd(
                    aggregateId = UUID.randomUUID()
            )

            val repoMock = mockk<AggregateRepository>().apply {
                every { read(someCmd.aggregateId, someAggregateConfiguration) } returns AggregateReadResult.NonExistingAggregate
                every { append("some-aggregate", someCmd.aggregateId, ExpectedEventNumber.AggregateDoesNotExist, any()) } returns
                        Unit
            }

            class EmployeeCmdHandler : CmdHandler<SomeAggregate>(repoMock, someAggregateConfiguration) {
                init {
                    init<SomeCmd> {
                        Result.Fail(IllegalStateException("some invalid state"))
                    }
                }
            }

            shouldThrow<IllegalStateException> { EmployeeCmdHandler().handle(someCmd) }
                    .message shouldBe "some invalid state"
        }

        "Test that a command \"RetryOrFail\" results in an exception being thrown" {
            data class SomeCmd(override val aggregateId: UUID, val newStartDate: LocalDate) : Cmd<SomeAggregate>

            val changeStartDate = SomeCmd(
                    aggregateId = UUID.randomUUID(),
                    newStartDate = LocalDate.now()
            )

            val repoMock = mockk<AggregateRepository>().apply {
                every { read(changeStartDate.aggregateId, someAggregateConfiguration) } returns AggregateReadResult.NonExistingAggregate
            }

            class EmployeeCmdHandler : CmdHandler<SomeAggregate>(repoMock, someAggregateConfiguration) {
                init {
                    init<SomeCmd> {
                        Result.RetryOrFail(IllegalStateException("some invalid state"))
                    }
                }
            }

            shouldThrow<IllegalStateException> { EmployeeCmdHandler().handle(changeStartDate) }
                    .message shouldBe "some invalid state"
        }
    }
}
