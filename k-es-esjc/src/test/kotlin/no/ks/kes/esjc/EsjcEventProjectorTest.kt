package no.ks.kes.esjc

import com.github.msemys.esjc.ResolvedEvent
import com.github.msemys.esjc.proto.EventStoreClientMessages
import com.github.msemys.esjc.proto.EventStoreClientMessages.EventRecord
import com.google.protobuf.ByteString
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.mockk
import no.ks.kes.esjc.jackson.JacksonEventSerdes
import no.ks.kes.esjc.testdomain.HiredEvent
import no.ks.kes.esjc.testdomain.StartDatesProjection
import java.time.Instant
import java.time.LocalDate
import java.util.*

internal class EsjcEventProjectorTest : StringSpec() {
    init {
        "Test that a registered projection is updated when a subscribed event is received" {
            val serdes = JacksonEventSerdes(setOf(HiredEvent::class))
            val startDatesProjection = StartDatesProjection()
            val event = HiredEvent(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now(),
                    timestamp = Instant.now()
            )

            val hired = EventRecord.newBuilder()
                    .setData(ByteString.copyFrom(serdes.serialize(event)))
                    .setDataContentType(1)
                    .setEventStreamId(UUID.randomUUID().toString())
                    .setEventNumber(0)
                    .setEventId(ByteString.copyFrom(UUID.randomUUID().toString(), "UTF-8"))
                    .setEventType("Hired")
                    .setMetadataContentType(1)
                    .build()

            EsjcEventListener(serdes, setOf(startDatesProjection), { }, { _, _ -> }).onEvent(mockk(), ResolvedEvent(EventStoreClientMessages.ResolvedIndexedEvent.newBuilder()
                    .setLink(hired)
                    .setEvent(hired)
                    .build()))

            startDatesProjection.getStartDate(event.aggregateId) shouldBe event.startDate
        }
    }
}