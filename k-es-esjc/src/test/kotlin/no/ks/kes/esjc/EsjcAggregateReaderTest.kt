package no.ks.kes.esjc

import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.ResolvedEvent
import com.github.msemys.esjc.operation.StreamNotFoundException
import com.github.msemys.esjc.proto.EventStoreClientMessages
import com.google.protobuf.ByteString
import io.kotlintest.matchers.beInstanceOf
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import no.ks.kes.lib.*
import java.time.Instant
import java.util.*
import java.util.stream.Stream

class EsjcAggregateReaderTest : StringSpec() {
    data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    @SerializationId("some-id")
    data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>


    @SerializationId("some-other-id")
    data class SomeOtherEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>


    private val someAggregateConfiguration = object : AggregateConfiguration<SomeAggregate>("some-aggregate") {
        init {
            init<SomeEvent> {
                SomeAggregate(stateInitialized = true)
            }
            apply<SomeOtherEvent> {
                copy(stateUpdated = true)
            }
        }
    }

    init {
        "Test that the reader can retrieve and deserialize aggregate events from the event-store" {
            val someEvent = SomeEvent(UUID.randomUUID(), Instant.now())
            val someOtherEvent = SomeOtherEvent(UUID.randomUUID(), Instant.now())
            val eventStoreMock = mockk<EventStore>()
                    .apply {
                        every { streamEventsForward(any(), any(), any(), any()) } returns
                                Stream.of(ResolvedEvent(EventStoreClientMessages.ResolvedIndexedEvent.newBuilder()
                                        .setEvent(EventStoreClientMessages.EventRecord.newBuilder()
                                                .setData(ByteString.copyFrom("some-id".toByteArray()))
                                                .setDataContentType(1)
                                                .setEventStreamId(UUID.randomUUID().toString())
                                                .setEventNumber(0)
                                                .setEventId(ByteString.copyFrom(UUID.randomUUID().toString(), "UTF-8"))
                                                .setEventType("some-id")
                                                .setMetadataContentType(1)
                                                .build())
                                        .build()),
                                        ResolvedEvent(EventStoreClientMessages.ResolvedIndexedEvent.newBuilder()
                                                .setEvent(EventStoreClientMessages.EventRecord.newBuilder()
                                                        .setData(ByteString.copyFrom("some-other-id".toByteArray()))
                                                        .setDataContentType(1)
                                                        .setEventStreamId(UUID.randomUUID().toString())
                                                        .setEventNumber(0)
                                                        .setEventId(ByteString.copyFrom(UUID.randomUUID().toString(), "UTF-8"))
                                                        .setEventType("some-other-id")
                                                        .setMetadataContentType(1)
                                                        .build())
                                                .build()))
                    }

            val deserializer = mockk<EventSerdes<String>>().apply {
                every { deserialize("some-id", any()) } returns someEvent
                every { deserialize("some-other-id", any()) } returns someOtherEvent
            }

            EsjcAggregateRepository(
                    eventStore = eventStoreMock,
                    deserializer = deserializer,
                    streamIdGenerator = { t, id -> "$t.$id" }
            )
                    .read(UUID.randomUUID(), someAggregateConfiguration)
                    .apply {
                        with(this as AggregateReadResult.ExistingAggregate<SomeAggregate>) {
                            aggregateState.stateInitialized shouldBe true
                            aggregateState.stateUpdated shouldBe true
                        }
                    }
        }

        "Test that the reader returns an empty aggregate if no stream is found" {
            EsjcAggregateRepository(
                    eventStore = mockk<EventStore>()
                            .apply {
                                every<Stream<ResolvedEvent>?> { streamEventsForward(any(), any(), any(), any()) } throws
                                        StreamNotFoundException("some message")
                            },
                    deserializer = mockk(),
                    streamIdGenerator = { t, id -> "$t.$id" }
            )
                    .read(UUID.randomUUID(), someAggregateConfiguration)
                    .apply {
                        this should beInstanceOf<AggregateReadResult.NonExistingAggregate>()
                    }
        }

    }
}