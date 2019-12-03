package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.ks.kes.lib.EventUpgrader.upgradeTo
import java.util.*

internal class EventUpgraderTest : StringSpec() {

    init {
        "Test that the upgrade method of an event is executed, and that the upgraded event is returned" {
            val oldEvent = OldEvent(UUID.randomUUID(), 0L)
            upgradeTo(oldEvent, NewEvent::class).apply {
                aggregateId shouldBe oldEvent.aggregateId
                timestamp shouldBe oldEvent.timestamp
            }
        }
    }

    private data class OldEvent(override val aggregateId: UUID, override val timestamp: Long) : Event {
        override fun upgrade(): Event? = NewEvent(aggregateId, timestamp)
    }

    private data class NewEvent(override val aggregateId: UUID, override val timestamp: Long) : Event
}