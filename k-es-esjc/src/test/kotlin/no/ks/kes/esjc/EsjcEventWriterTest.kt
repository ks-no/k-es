package no.ks.kes.esjc

import com.github.msemys.esjc.EventData
import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.Position
import com.github.msemys.esjc.WriteResult
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import no.ks.kes.lib.*
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletableFuture

internal class EsjcEventWriterTest : StringSpec() {

    init {
        "Test at esjc writer blir korrekt invokert under skriving av hendelser til aggregat" {
            data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            @SerializationId("some-id")
            data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>

            val eventAggregateType = UUID.randomUUID().toString()

            val event = SomeEvent(UUID.randomUUID(), Instant.now())

            val capturedEventData = slot<List<EventData>>()
            val eventStoreMock = mockk<EventStore>().apply {
                every { appendToStream("ks.fiks.$eventAggregateType.${event.aggregateId}", 0L, capture(capturedEventData)) } returns
                        CompletableFuture.completedFuture(WriteResult(0, Position(0L, 0L)))
            }

            val deserializer = mockk<EventSerdes<String>>().apply { every { serialize(event) } returns "some-id"}
            val esjcEventWriter = EsjcAggregateRepository(
                    eventStore = eventStoreMock,
                    streamIdGenerator = { t: String, id: UUID -> "ks.fiks.$t.$id" },
                    deserializer = deserializer)

            esjcEventWriter.append(eventAggregateType, event.aggregateId, ExpectedEventNumber.Exact(0L), listOf(event))

            with(capturedEventData.captured.single()) {
                this.data!! contentEquals  "foo".toByteArray()
                isJsonData shouldBe true
                type shouldBe "some-id"
                eventId shouldNotBe null
            }

        }
    }

}