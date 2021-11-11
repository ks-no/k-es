package no.ks.kes.grpc

import com.eventstore.dbclient.*
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

internal class GrpcEventWriterTest : StringSpec() {

    init {
        "Test that the grpc writer is correctly invoked during the writing of events to the aggregate" {
            data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            data class SomeEventData(val aggregateId: UUID) : no.ks.kes.lib.EventData<SomeAggregate>

            val eventAggregateType = UUID.randomUUID().toString()

            val aggregateId = UUID.randomUUID()
            val event: Event<SomeEventData> =
                Event(aggregateId = aggregateId, eventData = SomeEventData(aggregateId))

            val capturedEventData = slot<Iterator<com.eventstore.dbclient.EventData>>()
            val eventStoreMock = mockk<EventStoreDBClient>().apply {
                every { appendToStream("ks.fiks.$eventAggregateType.${event.aggregateId}", any(), capture(capturedEventData)) } returns
                        CompletableFuture.completedFuture(WriteResult(StreamRevision.START, Position.START))
            }

            val serializer = mockk<EventSerdes>()
                    .apply {
                        every { isJson() } returns true
                        every { serialize(event.eventData) } returns "foo".toByteArray()
                        every { getSerializationId(any<KClass<no.ks.kes.lib.EventData<*>>>()) } answers { firstArg<KClass<no.ks.kes.lib.EventData<*>>>().simpleName!! }
                    }
            val grpcEventWriter = GrpcAggregateRepository(
                    eventStoreDBClient = eventStoreMock,
                    streamIdGenerator = { t: String, id: UUID -> "ks.fiks.$t.$id" },
                    serdes = serializer)

            grpcEventWriter.append(eventAggregateType, event.aggregateId, ExpectedEventNumber.Exact(0L), listOf(event))
            with(capturedEventData.captured.next()) {
                Assertions.assertArrayEquals( "foo".toByteArray(), this.eventData!!)
                this.contentType shouldBe "application/json"
                this.eventType shouldBe "SomeEventData"
                eventId shouldNotBe null
            }

        }
    }

}