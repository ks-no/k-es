package no.ks.kes.lib

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import java.time.LocalDate
import java.util.*
import java.util.concurrent.Executors
import kotlin.reflect.KClass

private const val AGGREGATE_TYPE = "some-aggregate"

internal class CmdHandlerTest : StringSpec() {
    data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    data class SomeEvent(override val aggregateId: UUID) : Event<SomeAggregate>

    val someAggregateConfiguration = object : AggregateConfiguration<SomeAggregate>(AGGREGATE_TYPE) {
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

            val aggregateConfiguration = mockk<AggregateConfiguration<SomeAggregate>>()
                    .apply {
                        every { getConfiguration(any()) } returns mockk()
                    }

            shouldThrow<IllegalStateException> {
                object : CmdHandler<SomeAggregate>(mockk(), aggregateConfiguration) {
                    init {
                        init<HireCmd> {
                            Result.Succeed(SomeEvent(it.aggregateId))
                        }

                        init<HireCmd> {
                            Result.Succeed(SomeEvent(it.aggregateId))
                        }
                    }
                }
            }.apply {
                message shouldBe "There are multiple \"init\" configurations for the command HireCmd in the command handler null, only a single \"init\" handler is allowed for each command"
            }
        }

        "Test that multiple \"apply\" handlers results in an exception being thrown" {
            data class HireCmd(override val aggregateId: UUID, val startDate: LocalDate) : Cmd<SomeAggregate>

            val aggregateConfiguration = mockk<AggregateConfiguration<SomeAggregate>>()
                    .apply {
                        every { getConfiguration(any()) } returns mockk()
                    }

            shouldThrow<IllegalStateException> {
                object : CmdHandler<SomeAggregate>(mockk(), aggregateConfiguration) {
                    init {
                        apply<HireCmd> {
                            Result.Succeed(SomeEvent(it.aggregateId))
                        }

                        apply<HireCmd> {
                            Result.Succeed(SomeEvent(it.aggregateId))
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
                every { read(someCmd.aggregateId, any<ValidatedAggregateConfiguration<*>>()) } returns
                        AggregateReadResult.InitializedAggregate(SomeAggregate(true), 0)
                every { append(AGGREGATE_TYPE, someCmd.aggregateId, ExpectedEventNumber.Exact(0), any()) } returns
                        Unit
                every { getSerializationId(any()) } answers { firstArg<KClass<Event<*>>>().simpleName!! }
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

        "Test that multiple threads may issue commands that will results in events being applied to an existing aggregate" {
            data class SomeCmd(override val aggregateId: UUID) : Cmd<SomeAggregate>

            val someCmd = SomeCmd(
                    aggregateId = UUID.randomUUID()
            )


            val repoMock = mockk<AggregateRepository>().apply {
                every { read(someCmd.aggregateId, any<ValidatedAggregateConfiguration<*>>()) } returns
                        AggregateReadResult.InitializedAggregate(SomeAggregate(true), 0)
                every { append(AGGREGATE_TYPE, someCmd.aggregateId, ExpectedEventNumber.Exact(0), any()) } returns
                        Unit
                every { getSerializationId(any()) } answers { firstArg<KClass<Event<*>>>().simpleName!! }
            }

            val countingCommandHandler = object : CmdHandler<SomeAggregate>(repoMock, someAggregateConfiguration) {
                init {
                    apply<SomeCmd> {
                        Result.Succeed(SomeEvent(it.aggregateId))
                    }

                }
            }

            countingCommandHandler.handle(someCmd).apply {
                stateInitialized shouldBe true
            }
            Executors.newFixedThreadPool(10).asCoroutineDispatcher().use { dispatcher ->
                awaitAll(
                        async(dispatcher) { countingCommandHandler.handleUnsynchronized(someCmd) },
                        async(dispatcher) { countingCommandHandler.handleUnsynchronized(someCmd) },
                        async(dispatcher) { countingCommandHandler.handleUnsynchronized(someCmd) },
                        async(dispatcher) { countingCommandHandler.handleUnsynchronized(someCmd) },
                        async(dispatcher) { countingCommandHandler.handleUnsynchronized(someCmd) },
                        async(dispatcher) { countingCommandHandler.handleUnsynchronized(someCmd) },
                        async(dispatcher) { countingCommandHandler.handleUnsynchronized(someCmd) },
                        async(dispatcher) { countingCommandHandler.handleUnsynchronized(someCmd) },
                        async(dispatcher) { countingCommandHandler.handleUnsynchronized(someCmd) },
                        async(dispatcher) { countingCommandHandler.handleUnsynchronized(someCmd) }
                        )
            }
            verify(exactly = 15) {
                repoMock.getSerializationId(any())
            }
            verify(exactly = 11) {
                repoMock.append(AGGREGATE_TYPE, someCmd.aggregateId, ExpectedEventNumber.Exact(0), any())
                repoMock.read(someCmd.aggregateId, any<ValidatedAggregateConfiguration<*>>())
            }

        }

        "Test that a command \"Fail\" results in an exception being thrown" {
            data class SomeCmd(override val aggregateId: UUID) : Cmd<SomeAggregate>

            val someCmd = SomeCmd(
                    aggregateId = UUID.randomUUID()
            )

            val repoMock = mockk<AggregateRepository>().apply {
                every { read(someCmd.aggregateId, any<ValidatedAggregateConfiguration<*>>()) } returns AggregateReadResult.NonExistingAggregate
                every { append(AGGREGATE_TYPE, someCmd.aggregateId, ExpectedEventNumber.AggregateDoesNotExist, any()) } returns
                        Unit
                every { getSerializationId(any()) } answers { firstArg<KClass<Event<*>>>().simpleName!! }
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
