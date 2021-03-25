package no.ks.kes.lib

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import no.ks.kes.lib.EventUpgrader.upgradeTo
import java.util.*

internal class EventUpgraderTest : StringSpec() {

    init {
        "Test that the upgrade method of an event is executed, and that the upgraded event is returned" {
            data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate
            data class NewEventData(val aggregateId: UUID) : EventData<SomeAggregate>
            data class OldEventData(val aggregateId: UUID) : EventData<SomeAggregate> {
                override fun upgrade(): EventData<SomeAggregate>? = NewEventData(aggregateId)
            }

            val oldEvent = OldEventData(UUID.randomUUID())
            upgradeTo(oldEvent, NewEventData::class).apply {
                aggregateId shouldBe oldEvent.aggregateId
            }
        }
    }
}