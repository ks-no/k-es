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
        "Test that the esjc writer is correctly invoked during the writing of events to the aggregate" {
            data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            data class SomeEventData(val aggregateId: UUID) : no.ks.kes.lib.EventData<SomeAggregate>

            val eventAggregateType = UUID.randomUUID().toString()

            val aggregateId = UUID.randomUUID()
            val event: no.ks.kes.lib.Event =
                Event(aggregateId = aggregateId, eventData = SomeEventData(aggregateId))

            val capturedEventData = slot<List<EventData>>()
            val eventStoreMock = mockk<EventStore>().apply {
                every { appendToStream("ks.fiks.$eventAggregateType.${event.aggregateId}", 0L, capture(capturedEventData)) } returns
                        CompletableFuture.completedFuture(WriteResult(0, Position(0L, 0L)))
            }

            val deserializer = mockk<EventSerdes>()
                    .apply {
                        every { isJson() } returns true
                        every { serialize(event.eventData) } returns "foo".toByteArray()
                        every { getSerializationId(any<KClass<no.ks.kes.lib.EventData<*>>>()) } answers { firstArg<KClass<no.ks.kes.lib.EventData<*>>>().simpleName!! }
                    }
            val esjcEventWriter = EsjcAggregateRepository(
                    eventStore = eventStoreMock,
                    streamIdGenerator = { t: String, id: UUID -> "ks.fiks.$t.$id" },
                    serdes = deserializer)

            esjcEventWriter.append(eventAggregateType, event.aggregateId, ExpectedEventNumber.Exact(0L), listOf(event))

            with(capturedEventData.captured.single()) {
                Assertions.assertArrayEquals( "foo".toByteArray(), this.data!!)
                isJsonData shouldBe true
                type shouldBe "SomeEventData"
                eventId shouldNotBe null
            }

        }
    }

}