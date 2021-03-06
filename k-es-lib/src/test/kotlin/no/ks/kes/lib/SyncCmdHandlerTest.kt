package no.ks.kes.lib

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import java.time.LocalDate
import java.util.*
import kotlin.random.Random
import kotlin.reflect.KClass

internal class SyncCmdHandlerTest : StringSpec() {
    data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    data class SomeEventData(val aggregateId: UUID) : EventData<SomeAggregate>

    val someAggregateConfiguration = object : AggregateConfiguration<SomeAggregate>("some-aggregate") {
        init {
            init { someEvent: SomeEventData, aggregateId: UUID ->
                SomeAggregate(stateInitialized = true)
            }

            apply<SomeEventData> {
                copy(stateUpdated = true)
            }
        }
    }

    init {
        "Test that a cmd can initialize a non-existing aggregate, and that the derived state is returned" {
            data class HireCmd(override val aggregateId: UUID, val startDate: LocalDate) : Cmd<SomeAggregate>

            val hireCmd = HireCmd(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now()
            )

            val repoMock = mockk<AggregateRepository>().apply {
                every { read(hireCmd.aggregateId, any<ValidatedAggregateConfiguration<*>>()) } returns AggregateReadResult.NonExistingAggregate
                every { getSerializationId(any()) } answers { firstArg<KClass<EventData<*>>>().simpleName!! }
                every { append("some-aggregate", hireCmd.aggregateId, ExpectedEventNumber.AggregateDoesNotExist, any()) } returns
                        Unit
            }

            object : CmdHandler<SomeAggregate>(repoMock, someAggregateConfiguration) {
                init {
                    init<HireCmd> {
                        Result.Succeed(Event(it.aggregateId, SomeEventData(it.aggregateId)))
                    }
                }
            }.handle(hireCmd).apply { stateInitialized shouldBe true }
        }

        "Test that a cmd can initialize a uninitialized aggregate, and that the derived state is returned" {
            data class HireCmd(override val aggregateId: UUID, val startDate: LocalDate) : Cmd<SomeAggregate>

            val hireCmd = HireCmd(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now()
            )

            val eventNumber = Random.nextLong(0L, 10000000L)
            val repoMock = mockk<AggregateRepository>().apply {
                every { read(hireCmd.aggregateId, any<ValidatedAggregateConfiguration<*>>()) } returns AggregateReadResult.UninitializedAggregate(eventNumber)
                every { getSerializationId(any()) } answers { firstArg<KClass<EventData<*>>>().simpleName!! }
                every { append("some-aggregate", hireCmd.aggregateId, ExpectedEventNumber.Exact(eventNumber), any()) } returns
                        Unit
            }

            object : CmdHandler<SomeAggregate>(repoMock, someAggregateConfiguration) {
                init {
                    init<HireCmd> {
                        Result.Succeed(Event(it.aggregateId, SomeEventData(it.aggregateId)))
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
                every { read(someCmd.aggregateId, any<ValidatedAggregateConfiguration<*>>()) } returns
                        AggregateReadResult.InitializedAggregate(SomeAggregate(true), 0)
                every { getSerializationId(any()) } answers { firstArg<KClass<EventData<*>>>().simpleName!! }
                every { append("some-aggregate", someCmd.aggregateId, ExpectedEventNumber.Exact(0), any()) } returns
                        Unit
            }

            object : CmdHandler<SomeAggregate>(repoMock, someAggregateConfiguration) {
                init {
                    apply<SomeCmd> {
                        Result.Succeed(Event(it.aggregateId, SomeEventData(it.aggregateId)))
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
                every { read(someCmd.aggregateId, any<ValidatedAggregateConfiguration<*>>()) } returns AggregateReadResult.NonExistingAggregate
                every { getSerializationId(any()) } answers { firstArg<KClass<EventData<*>>>().simpleName!! }
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
                every { read(changeStartDate.aggregateId, any<ValidatedAggregateConfiguration<*>>()) } returns AggregateReadResult.NonExistingAggregate
                every { getSerializationId(any()) } answers { firstArg<KClass<EventData<*>>>().simpleName!! }
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
