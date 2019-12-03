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
import no.ks.kes.esjc.jackson.JacksonEventSerdes
import no.ks.kes.esjc.testdomain.HiredEvent
import java.time.LocalDate
import java.util.*
import java.util.concurrent.CompletableFuture

internal class EsjcEventWriterTest : StringSpec() {

    init {
        "Test at esjc writer blir korrekt invokert under skriving av hendelser til aggregat" {
            val eventAggregateType = UUID.randomUUID().toString()

            val event = HiredEvent(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now()
            )

            val capturedEventData = slot<List<EventData>>()
            val eventStoreMock = mockk<EventStore>().apply {
                every { appendToStream("ks.fiks.$eventAggregateType.${event.aggregateId}", 0L, capture(capturedEventData)) } returns
                        CompletableFuture.completedFuture(WriteResult(0, Position(0L, 0L)))
            }

            val serdes = JacksonEventSerdes(setOf(HiredEvent::class))
            val esjcEventWriter = EsjcEventWriter(eventStoreMock, { t: String, id: UUID -> "ks.fiks.$t.$id" }, serdes)

            esjcEventWriter.write(eventAggregateType, event.aggregateId, 0L, listOf(event), true)

            with(capturedEventData.captured.single()) {
                this.data!!.contentEquals(serdes.serialize(event)) shouldBe true
                isJsonData shouldBe true
                type shouldBe "Hired"
                eventId shouldNotBe null
            }

        }
    }

}