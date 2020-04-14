package no.ks.kes.lib

import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import java.time.Instant
import java.util.*

internal class AsyncCmdHandlerTest : StringSpec() {
    data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    @SerializationId("some-id")
    data class SomeInitEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>

    @SerializationId("some-other-id")
    data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>

    val aggregateConfig = object : AggregateConfiguration<SomeAggregate>("some-aggregate") {
        init {
            init<SomeInitEvent> {
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

            val slot = slot<List<Event<*>>>()

            val repoMock = mockk<AggregateRepository>().apply {
                every { read(someCmd.aggregateId, ofType<AggregateConfiguration<*>>()) } returns AggregateReadResult.NonExistingAggregate
                every { append("some-aggregate", someCmd.aggregateId, ExpectedEventNumber.AggregateDoesNotExist, capture(slot)) } returns Unit
            }

            object : CmdHandler<SomeAggregate>(repoMock, aggregateConfig) {
                init {
                    init<SomeCmd> {
                        Result.Succeed(SomeInitEvent(it.aggregateId, Instant.now()))
                    }
                }
            }.handleAsync(someCmd, 0)
                    .apply { this.shouldBeInstanceOf<CmdHandler.AsyncResult.Success>() }

            with(slot.captured.single() as SomeInitEvent) {
                aggregateId shouldBe someCmd.aggregateId
            }
        }

        "Test that a cmd can append a new event to an existing aggregate" {
            val someCmd = SomeCmd(UUID.randomUUID())
            val slot = slot<List<Event<*>>>()

            val repoMock = mockk<AggregateRepository>().apply {
                every { read(someCmd.aggregateId, ofType<AggregateConfiguration<*>>()) } returns
                        AggregateReadResult.ExistingAggregate(SomeAggregate(true), 0)
                every { append("some-aggregate", someCmd.aggregateId, ExpectedEventNumber.Exact(0), capture(slot)) } returns Unit
            }

            object : CmdHandler<SomeAggregate>(repoMock, aggregateConfig) {
                init {
                    apply<SomeCmd> {
                        Result.Succeed(SomeEvent(it.aggregateId, Instant.now()))
                    }
                }
            }.handleAsync(someCmd, 0)
                    .apply { this.shouldBeInstanceOf<CmdHandler.AsyncResult.Success>() }

            with(slot.captured.single() as SomeEvent) {
                aggregateId shouldBe someCmd.aggregateId
            }
        }

    }
}
