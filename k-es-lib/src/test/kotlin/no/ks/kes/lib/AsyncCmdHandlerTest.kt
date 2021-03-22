package no.ks.kes.lib

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import java.util.*
import kotlin.reflect.KClass

internal class AsyncCmdHandlerTest : StringSpec() {
    data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    class SomeInitEvent : Event<SomeAggregate>

    class SomeEvent : Event<SomeAggregate>

    val aggregateConfig = object : AggregateConfiguration<SomeAggregate>("some-aggregate") {
        init {
            init { someInitEvent: SomeInitEvent, aggregateId: UUID ->
                SomeAggregate(stateInitialized = true)
            }

            apply<SomeEvent> {
                copy(stateUpdated = true)
            }
        }
    }

    data class SomeCmd(override val aggregateId: UUID) : Cmd<SomeAggregate>

    init {
        "Test that a cmd can initialize an aggregate" {
            val someCmd = SomeCmd(UUID.randomUUID())

            val slot = slot<List<EventData>>()

            val repoMock = mockk<AggregateRepository>().apply {
                every { getSerializationId(any()) } answers { firstArg<KClass<Event<*>>>().simpleName!! }
                every { read(someCmd.aggregateId, ofType<ValidatedAggregateConfiguration<*>>()) } returns AggregateReadResult.NonExistingAggregate
                every { append("some-aggregate", someCmd.aggregateId, ExpectedEventNumber.AggregateDoesNotExist, capture(slot)) } returns Unit
            }

            object : CmdHandler<SomeAggregate>(repoMock, aggregateConfig) {
                init {
                    init<SomeCmd> {
                        Result.Succeed(EventData( event = SomeInitEvent(), aggregateId = it.aggregateId))
                    }
                }
            }.handleAsync(someCmd, 0)
                    .apply { this.shouldBeInstanceOf<CmdHandler.AsyncResult.Success>() }

            with(slot.captured.single() as EventData) {
                aggregateId shouldBe someCmd.aggregateId
            }
        }

        "Test that a cmd can append a new event to an existing aggregate" {
            val someCmd = SomeCmd(UUID.randomUUID())
            val slot = slot<List<EventData>>()

            val repoMock = mockk<AggregateRepository>().apply {
                every { getSerializationId(any()) } answers { firstArg<KClass<Event<*>>>().simpleName!! }
                every { read(someCmd.aggregateId, ofType<ValidatedAggregateConfiguration<*>>()) } returns
                        AggregateReadResult.InitializedAggregate(SomeAggregate(true), 0)
                every { append("some-aggregate", someCmd.aggregateId, ExpectedEventNumber.Exact(0), capture(slot)) } returns Unit
            }

            object : CmdHandler<SomeAggregate>(repoMock, aggregateConfig) {
                init {
                    apply<SomeCmd> {
                        Result.Succeed(EventData( event = SomeInitEvent(), aggregateId = it.aggregateId))
                    }
                }
            }.handleAsync(someCmd, 0)
                    .apply { this.shouldBeInstanceOf<CmdHandler.AsyncResult.Success>() }

            with(slot.captured.single() as EventData) {
                aggregateId shouldBe someCmd.aggregateId
            }
        }

    }
}
