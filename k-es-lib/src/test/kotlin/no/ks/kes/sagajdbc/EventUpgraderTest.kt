package no.ks.kes.sagajdbc

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.ks.kes.sagajdbc.EventUpgrader.upgradeTo
import no.ks.kes.sagajdbc.testdomain.Employee
import java.time.Instant
import java.util.*

internal class EventUpgraderTest : StringSpec() {

    init {
        "Test that the upgrade method of an event is executed, and that the upgraded event is returned" {
            val oldEvent = OldEvent(UUID.randomUUID(), Instant.now())
            upgradeTo(oldEvent, NewEvent::class).apply {
                aggregateId shouldBe oldEvent.aggregateId
                timestamp shouldBe oldEvent.timestamp
            }
        }
    }

    private data class OldEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<Employee> {
        override fun upgrade(): Event<Employee>? = NewEvent(aggregateId, timestamp)
    }

    private data class NewEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<Employee>
}