package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.ks.kes.lib.testdomain.HiredEvent
import no.ks.kes.lib.testdomain.StartDatesProjection
import java.time.LocalDate
import java.util.*

internal class ProjectionTest : StringSpec() {

    init {
        "test that a projection can handle incoming events and mutate its state accordingly" {
            val hiredEvent = HiredEvent(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now())

            StartDatesProjection().apply {
                accept(EventWrapper(hiredEvent, 0))

                getStartDate(hiredEvent.aggregateId) shouldBe LocalDate.now()
            }
        }
    }
}