package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import java.time.Instant
import java.time.LocalDate
import java.util.*
import kotlin.reflect.KClass

internal class SyncCmdHandlerTest : StringSpec() {
    data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    data class SomeEvent(override val aggregateId: UUID) : Event<SomeAggregate>

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
        "Test that a cmd can initialize an aggregate, and that the derived state is returned" {
            data class HireCmd(override val aggregateId: UUID, val startDate: LocalDate) : Cmd<SomeAggregate>

            val hireCmd = HireCmd(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now()
            )

            val repoMock = mockk<AggregateRepository>().apply {
                every { read(hireCmd.aggregateId, any<AggregateConfiguration.ValidatedAggregateConfiguration<*>>()) } returns AggregateReadResult.NonExistingAggregate
                every { getSerializationId(any()) } answers { firstArg<KClass<Event<*>>>().simpleName!! }
                every { append("some-aggregate", hireCmd.aggregateId, ExpectedEventNumber.AggregateDoesNotExist, any()) } returns
                        Unit
            }

            object : CmdHandler<SomeAggregate>(repoMock, someAggregateConfiguration) {
                init {
                    init<HireCmd> {
                        Result.Succeed(SomeEvent(it.aggregateId))
                    }
                }
            }.handle(hireCmd).apply { stateInitialized shouldBe true }
        }

        "Test that a command can result in an event being applied to an existing aggregate" {
            data class SomeCmd(override val aggregateId: UUID) : Cmd<SomeAggregate>

            val someCmd = SomeCmd(
                    aggregateId = UUID.randomUUID()
            )

            val repoMock = mockk<AggregateRepository>().apply {
                every { read(someCmd.aggregateId, any<AggregateConfiguration.ValidatedAggregateConfiguration<*>>()) } returns
                        AggregateReadResult.InitializedAggregate(SomeAggregate(true), 0)
                every { getSerializationId(any()) } answers { firstArg<KClass<Event<*>>>().simpleName!! }
                every { append("some-aggregate", someCmd.aggregateId, ExpectedEventNumber.Exact(0), any()) } returns
                        Unit
            }

            object : CmdHandler<SomeAggregate>(repoMock, someAggregateConfiguration) {
                init {
                    apply<SomeCmd> {
                        Result.Succeed(SomeEvent(it.aggregateId))
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
                every { read(someCmd.aggregateId, any<AggregateConfiguration.ValidatedAggregateConfiguration<*>>()) } returns AggregateReadResult.NonExistingAggregate
                every { getSerializationId(any()) } answers { firstArg<KClass<Event<*>>>().simpleName!! }
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
                every { read(changeStartDate.aggregateId, any<AggregateConfiguration.ValidatedAggregateConfiguration<*>>()) } returns AggregateReadResult.NonExistingAggregate
                every { getSerializationId(any()) } answers { firstArg<KClass<Event<*>>>().simpleName!! }
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
