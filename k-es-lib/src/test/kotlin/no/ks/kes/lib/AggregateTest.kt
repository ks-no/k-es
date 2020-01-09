package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.ks.kes.lib.testdomain.Employee
import no.ks.kes.lib.testdomain.HiredEvent
import no.ks.kes.lib.testdomain.StartDateChangedEvent
import java.time.Instant
import java.time.LocalDate
import java.util.*

internal class AggregateTest : StringSpec() {

    init {
        "Test that the state of the aggregate changes in accordance with the applied event" {
            val hiredEvent = HiredEvent(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now(),
                    timestamp = Instant.now())

            Employee()
                    .applyEvent(hiredEvent, 0)
                    .apply {
                        aggregateId shouldBe hiredEvent.aggregateId
                        startDate shouldBe hiredEvent.startDate
                        currentEventNumber shouldBe 0
                    }
        }

        "Test that we can apply multiple events"{
            val hiredEvent = HiredEvent(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now(),
                    timestamp = Instant.now())

            val startDateChangedEvent = StartDateChangedEvent(
                    aggregateId = UUID.randomUUID(),
                    newStartDate = LocalDate.now(),
                    timestamp = Instant.now())

            Employee()
                    .applyEvent(hiredEvent, 0)
                    .applyEvent(startDateChangedEvent, 1)
                    .apply {
                        aggregateId shouldBe hiredEvent.aggregateId
                        startDate shouldBe startDateChangedEvent.newStartDate
                        currentEventNumber shouldBe 1
                    }
        }
    }
}