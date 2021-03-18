package no.ks.kes.esjc

import com.github.msemys.esjc.EventData
import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.Position
import com.github.msemys.esjc.WriteResult
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import no.ks.kes.lib.*
import org.junit.jupiter.api.Assertions
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.reflect.KClass

internal class EsjcEventWriterTest : StringSpec() {

    init {
        "Test at esjc writer blir korrekt invokert under skriving av hendelser til aggregat" {
            data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            data class SomeEvent(val aggregateId: UUID) : Event<SomeAggregate>

            val eventAggregateType = UUID.randomUUID().toString()

            val aggregateId = UUID.randomUUID()
            val eventWrapper: WriteEventWrapper<Event<*>> = WriteEventWrapper(aggregateId = aggregateId, event = SomeEvent(aggregateId), metadata = EventMetadata())

            val capturedEventData = slot<List<EventData>>()
            val eventStoreMock = mockk<EventStore>().apply {
                every { appendToStream("ks.fiks.$eventAggregateType.${eventWrapper.aggregateId}", 0L, capture(capturedEventData)) } returns
                        CompletableFuture.completedFuture(WriteResult(0, Position(0L, 0L)))
            }

            val deserializer = mockk<EventSerdes>()
                    .apply {
                        every { isJson() } returns true
                        every { serialize(eventWrapper.event) } returns "foo".toByteArray()
                        every { getSerializationId(any<KClass<Event<*>>>()) } answers { firstArg<KClass<Event<*>>>().simpleName!! }
                    }
            val esjcEventWriter = EsjcAggregateRepository(
                    eventStore = eventStoreMock,
                    streamIdGenerator = { t: String, id: UUID -> "ks.fiks.$t.$id" },
                    serdes = deserializer)

            esjcEventWriter.append(eventAggregateType, eventWrapper.aggregateId, ExpectedEventNumber.Exact(0L), listOf(eventWrapper))

            with(capturedEventData.captured.single()) {
                Assertions.assertArrayEquals( "foo".toByteArray(), this.data!!)
                isJsonData shouldBe true
                type shouldBe "SomeEvent"
                eventId shouldNotBe null
            }

        }
    }

}