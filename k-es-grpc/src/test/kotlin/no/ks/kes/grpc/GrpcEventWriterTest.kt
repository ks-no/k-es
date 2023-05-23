package no.ks.kes.grpc

import com.eventstore.dbclient.*
import com.eventstore.dbclient.EventData
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import no.ks.kes.lib.*
import org.junit.jupiter.api.Assertions
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
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
                        CompletableFuture.completedFuture(mockk { every { nextExpectedRevision } returns ExpectedRevision.any() })
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


        "Test that the grpc writer retries write if timeout caught at first write and allowRetry is true" {
            data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            data class SomeEventData(val aggregateId: UUID) : no.ks.kes.lib.EventData<SomeAggregate>

            val eventAggregateType = UUID.randomUUID().toString()

            val aggregateId = UUID.randomUUID()
            val event: Event<SomeEventData> =
                Event(aggregateId = aggregateId, eventData = SomeEventData(aggregateId))

            val capturedEventData = slot<Iterator<com.eventstore.dbclient.EventData>>()
            val eventStoreMock = mockk<EventStoreDBClient>().apply {
                every { appendToStream("ks.fiks.$eventAggregateType.${event.aggregateId}", any(), capture(capturedEventData)) }.throws(
                    ExecutionException(StatusRuntimeException(Status.ABORTED))
                ).andThen(CompletableFuture.completedFuture(mockk { every { nextExpectedRevision } returns ExpectedRevision.any() }))
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
                serdes = serializer,
                allowRetryOnWrite = true)

            grpcEventWriter.append(eventAggregateType, event.aggregateId, ExpectedEventNumber.Exact(0L), listOf(event))
            with(capturedEventData.captured.next()) {
                Assertions.assertArrayEquals( "foo".toByteArray(), this.eventData!!)
                this.contentType shouldBe "application/json"
                this.eventType shouldBe "SomeEventData"
                eventId shouldNotBe null
            }


            verify(exactly = 2) { eventStoreMock.appendToStream("ks.fiks.$eventAggregateType.${event.aggregateId}", any<AppendToStreamOptions>(), any<Iterator<EventData>>()) }

        }

        "Test that the grpc writer throws WriteAbortedException if allowRetry is false, and StatusRuntimeException with Status.ABORTED is caught on append operation" {
            data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            data class SomeEventData(val aggregateId: UUID) : no.ks.kes.lib.EventData<SomeAggregate>

            val eventAggregateType = UUID.randomUUID().toString()

            val aggregateId = UUID.randomUUID()
            val event: Event<SomeEventData> =
                Event(aggregateId = aggregateId, eventData = SomeEventData(aggregateId))

            val capturedEventData = slot<Iterator<EventData>>()
            val eventStoreMock = mockk<EventStoreDBClient>().apply {
                every { appendToStream("ks.fiks.$eventAggregateType.${event.aggregateId}", any(), capture(capturedEventData)) }.throws(
                    ExecutionException(StatusRuntimeException(Status.ABORTED))
                ).andThen(CompletableFuture.completedFuture(mockk { every { nextExpectedRevision } returns ExpectedRevision.any() }))
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
                serdes = serializer,
                allowRetryOnWrite = false)

            shouldThrow<WriteAbortedException> {
                grpcEventWriter.append(
                    eventAggregateType,
                    event.aggregateId,
                    ExpectedEventNumber.Exact(0L),
                    listOf(event)
                )
            }

            verify(exactly = 1) { eventStoreMock.appendToStream("ks.fiks.$eventAggregateType.${event.aggregateId}", any<AppendToStreamOptions>(), any<Iterator<EventData>>()) }
        }

        "Test that the grpc writer throws WriteAbortedException if allowRetry is true, and timeout exception caught on retry " {
            data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            data class SomeEventData(val aggregateId: UUID) : no.ks.kes.lib.EventData<SomeAggregate>

            val eventAggregateType = UUID.randomUUID().toString()

            val aggregateId = UUID.randomUUID()
            val event: Event<SomeEventData> =
                Event(aggregateId = aggregateId, eventData = SomeEventData(aggregateId))

            val capturedEventData = slot<Iterator<EventData>>()
            val eventStoreMock = mockk<EventStoreDBClient>().apply {
                every { appendToStream("ks.fiks.$eventAggregateType.${event.aggregateId}", any(), capture(capturedEventData)) }.throws(
                    ExecutionException(StatusRuntimeException(Status.ABORTED)))
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
                serdes = serializer,
                allowRetryOnWrite = true)

            shouldThrow<WriteAbortedException> {
                grpcEventWriter.append(
                    eventAggregateType,
                    event.aggregateId,
                    ExpectedEventNumber.Exact(0L),
                    listOf(event)
                )
            }

            verify(exactly = 2) { eventStoreMock.appendToStream("ks.fiks.$eventAggregateType.${event.aggregateId}", any<AppendToStreamOptions>(), any<Iterator<EventData>>()) }
        }
    }

}