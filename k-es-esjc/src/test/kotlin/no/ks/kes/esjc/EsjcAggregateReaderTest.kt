
package no.ks.kes.esjc

import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.ResolvedEvent
import com.github.msemys.esjc.operation.StreamNotFoundException
import com.github.msemys.esjc.proto.EventStoreClientMessages
import com.google.protobuf.ByteString
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.beInstanceOf
import io.mockk.every
import io.mockk.mockk
import no.ks.kes.lib.*
import java.util.*
import java.util.stream.Stream
import kotlin.reflect.KClass

class EsjcAggregateReaderTest : StringSpec() {
    data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    data class SomeEventData(val aggregateId: UUID) : EventData<SomeAggregate>

    data class SomeOtherEventData(val aggregateId: UUID) : EventData<SomeAggregate>

    private val someAggregateConfiguration = object : AggregateConfiguration<SomeAggregate>("some-aggregate") {
        init {
            init { someEvent: SomeEventData, aggregatId: UUID ->
                SomeAggregate(stateInitialized = true)
            }
            apply<SomeOtherEventData> {
                copy(stateUpdated = true)
            }
        }
    }.getConfiguration { it.simpleName!! }

    init {
        "Test that the reader returns InitializedAggregate if a configured init-event is found in the stream" {
            val someEvent = SomeEventData(UUID.randomUUID())
            val someOtherEvent = SomeOtherEventData(UUID.randomUUID())
            val eventStoreMock = mockk<EventStore>()
                    .apply {
                        every { streamEventsForward(any(), any(), any(), any()) } returns
                                Stream.of(
                                        ResolvedEvent(EventStoreClientMessages.ResolvedIndexedEvent.newBuilder()
                                                .setEvent(EventStoreClientMessages.EventRecord.newBuilder()
                                                        .setData(ByteString.copyFrom("some-id".toByteArray()))
                                                        .setDataContentType(0)
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
                                                        .setEventNumber(1)
                                                        .setEventId(ByteString.copyFrom(UUID.randomUUID().toString(), "UTF-8"))
                                                        .setEventType("some-other-id")
                                                        .setMetadataContentType(1)
                                                        .build())
                                                .build())
                                )
                    }

            val deserializer = mockk<EventSerdes>().apply {
                every { deserialize("some-id".toByteArray(), any()) } returns someEvent
                every { getSerializationId(any<KClass<EventData<*>>>()) } answers { firstArg<KClass<EventData<*>>>().simpleName!! }
                every { deserialize("some-other-id".toByteArray(), any()) } returns someOtherEvent
            }

            EsjcAggregateRepository(
                    eventStore = eventStoreMock,
                    serdes = deserializer,
                    streamIdGenerator = { t, id -> "$t.$id" }
            )
                    .read(UUID.randomUUID(), someAggregateConfiguration)
                    .apply {
                        with(this as AggregateReadResult.InitializedAggregate<SomeAggregate>) {
                            aggregateState.stateInitialized shouldBe true
                            aggregateState.stateUpdated shouldBe true
                            eventNumber shouldBe 1L
                        }
                    }
        }

        "Test that the reader returns UninitializedAggregate if the stream consists solely of ignorable events" {
            val eventStoreMock = mockk<EventStore>()
                    .apply {
                        every { streamEventsForward(any(), any(), any(), any()) } returns
                                Stream.of(
                                        ResolvedEvent(EventStoreClientMessages.ResolvedIndexedEvent.newBuilder()
                                                .setEvent(EventStoreClientMessages.EventRecord.newBuilder()
                                                        .setData(ByteString.copyFrom("some-id".toByteArray()))
                                                        .setDataContentType(1)
                                                        .setEventStreamId(UUID.randomUUID().toString())
                                                        .setEventNumber(0)
                                                        .setEventId(ByteString.copyFrom(UUID.randomUUID().toString(), "UTF-8"))
                                                        .setEventType("\$some-id")
                                                        .setMetadataContentType(1)
                                                        .build())
                                                .build()),
                                        ResolvedEvent(EventStoreClientMessages.ResolvedIndexedEvent.newBuilder()
                                                .setEvent(EventStoreClientMessages.EventRecord.newBuilder()
                                                        .setData(ByteString.copyFrom("some-id".toByteArray()))
                                                        .setDataContentType(1)
                                                        .setEventStreamId(UUID.randomUUID().toString())
                                                        .setEventNumber(1)
                                                        .setEventId(ByteString.copyFrom(UUID.randomUUID().toString(), "UTF-8"))
                                                        .setEventType("\$some-other-id")
                                                        .setMetadataContentType(1)
                                                        .build())
                                                .build())
                                )
                    }

            EsjcAggregateRepository(
                    eventStore = eventStoreMock,
                    serdes = mockk(),
                    streamIdGenerator = { t, id -> "$t.$id" }
            )
                    .read(UUID.randomUUID(), someAggregateConfiguration)
                    .apply {
                        with(this as AggregateReadResult.UninitializedAggregate) {
                            this.eventNumber shouldBe 1L
                        }
                    }
        }

        "Test that the reader throws exception if the stream is empty" {
            val eventStoreMock = mockk<EventStore>()
                    .apply {
                        every { streamEventsForward(any(), any(), any(), any()) } returns Stream.of()
                    }

            shouldThrow<IllegalStateException> {
                EsjcAggregateRepository(
                        eventStore = eventStoreMock,
                        serdes = mockk(),
                        streamIdGenerator = { t, id -> "$t.$id" }
                )
                        .read(UUID.randomUUID(), someAggregateConfiguration)
            }.message.shouldContain("the stream exists but does not contain any events")
        }

        "Test that the reader returns a NonExistingAggregate if no stream is found" {
            EsjcAggregateRepository(
                    eventStore = mockk<EventStore>()
                            .apply {
                                every<Stream<ResolvedEvent>?> { streamEventsForward(any(), any(), any(), any()) } throws
                                        StreamNotFoundException("some message")
                            },
                    serdes = mockk(),
                    streamIdGenerator = { t, id -> "$t.$id" }
            )
                    .read(UUID.randomUUID(), someAggregateConfiguration)
                    .apply {
                        this should beInstanceOf<AggregateReadResult.NonExistingAggregate>()
                    }
        }
    }
}
