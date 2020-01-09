package no.ks.kes.esjc

import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.ResolvedEvent
import com.github.msemys.esjc.operation.StreamNotFoundException
import com.github.msemys.esjc.proto.EventStoreClientMessages
import com.google.protobuf.ByteString
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import no.ks.kes.esjc.jackson.JacksonEventSerdes
import no.ks.kes.esjc.testdomain.Employee
import no.ks.kes.esjc.testdomain.HiredEvent
import no.ks.kes.esjc.testdomain.StartDateChangedEvent
import java.time.LocalDate
import java.util.*
import java.util.stream.Stream

class EsjcAggregateReaderTest : StringSpec() {
    init {
        "Test that the reader can retrieve and deserialize aggregate events from the event-store" {
            val eventSerdes = JacksonEventSerdes(setOf(HiredEvent::class, StartDateChangedEvent::class))
            val hired = HiredEvent(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now()
            )
            val startDateChanged = StartDateChangedEvent(
                    aggregateId = UUID.randomUUID(),
                    newStartDate = LocalDate.now().plusDays(1)
            )

            val eventStoreMock = mockk<EventStore>()
                    .apply {
                        every { streamEventsForward(any(), any(), any(), any()) } returns
                                Stream.of(ResolvedEvent(EventStoreClientMessages.ResolvedIndexedEvent.newBuilder()
                                        .setEvent(EventStoreClientMessages.EventRecord.newBuilder()
                                                .setData(ByteString.copyFrom(eventSerdes.serialize(hired)))
                                                .setDataContentType(1)
                                                .setEventStreamId(UUID.randomUUID().toString())
                                                .setEventNumber(0)
                                                .setEventId(ByteString.copyFrom(UUID.randomUUID().toString(), "UTF-8"))
                                                .setEventType("Hired")
                                                .setMetadataContentType(1)
                                                .build())
                                        .build()),
                                        ResolvedEvent(EventStoreClientMessages.ResolvedIndexedEvent.newBuilder()
                                                .setEvent(EventStoreClientMessages.EventRecord.newBuilder()
                                                        .setData(ByteString.copyFrom(eventSerdes.serialize(startDateChanged)))
                                                        .setDataContentType(1)
                                                        .setEventStreamId(UUID.randomUUID().toString())
                                                        .setEventNumber(0)
                                                        .setEventId(ByteString.copyFrom(UUID.randomUUID().toString(), "UTF-8"))
                                                        .setEventType("StartDateChangedEvent")
                                                        .setMetadataContentType(1)
                                                        .build())
                                                .build()))
                    }

            EsjcAggregateReader(
                    eventStore = eventStoreMock,
                    deserializer = eventSerdes,
                    esjcStreamIdGenerator = { t, id -> "$t.$id" }
            )
                    .read(UUID.randomUUID(), Employee())
                    .apply {
                        aggregateId shouldBe hired.aggregateId
                        startDate shouldBe startDateChanged.newStartDate
                    }
        }

        "Test that the reader returns an empty aggregate if no stream is found" {
            EsjcAggregateReader(
                    eventStore = mockk<EventStore>()
                            .apply {
                                every<Stream<ResolvedEvent>?> { streamEventsForward(any(), any(), any(), any()) } throws
                                        StreamNotFoundException("some message")
                            },
                    deserializer = JacksonEventSerdes(setOf(HiredEvent::class, StartDateChangedEvent::class)),
                    esjcStreamIdGenerator = { t, id -> "$t.$id" }
            )
                    .read(UUID.randomUUID(), Employee())
                    .apply {
                        currentEventNumber shouldBe -1
                        startDate shouldBe null
                        aggregateId shouldBe null
                    }
        }

    }
}