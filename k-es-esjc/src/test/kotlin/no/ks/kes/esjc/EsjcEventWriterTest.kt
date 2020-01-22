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
import no.ks.kes.lib.EventSerdes
import no.ks.kes.lib.ExpectedEventNumber
import no.ks.kes.lib.testdomain.Hired
import java.time.Instant
import java.time.LocalDate
import java.util.*
import java.util.concurrent.CompletableFuture

internal class EsjcEventWriterTest : StringSpec() {

    init {
        "Test at esjc writer blir korrekt invokert under skriving av hendelser til aggregat" {
            val eventAggregateType = UUID.randomUUID().toString()

            val event = Hired(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now(),
                    timestamp = Instant.now(),
                    recruitedBy = UUID.randomUUID()
            )

            val capturedEventData = slot<List<EventData>>()
            val eventStoreMock = mockk<EventStore>().apply {
                every { appendToStream("ks.fiks.$eventAggregateType.${event.aggregateId}", 0L, capture(capturedEventData)) } returns
                        CompletableFuture.completedFuture(WriteResult(0, Position(0L, 0L)))
            }

            val deserializer = mockk<EventSerdes<String>>().apply { every { serialize(event) } returns "hired"}
            val esjcEventWriter = EsjcAggregateRepository(
                    eventStore = eventStoreMock,
                    streamIdGenerator = { t: String, id: UUID -> "ks.fiks.$t.$id" },
                    deserializer = deserializer)

            esjcEventWriter.write(eventAggregateType, event.aggregateId, ExpectedEventNumber.Exact(0L), listOf(event))

            with(capturedEventData.captured.single()) {
                this.data!! contentEquals  "foo".toByteArray()
                isJsonData shouldBe true
                type shouldBe "Hired"
                eventId shouldNotBe null
            }

        }
    }

}