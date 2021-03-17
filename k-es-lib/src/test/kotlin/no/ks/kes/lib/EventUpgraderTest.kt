package no.ks.kes.lib

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import no.ks.kes.lib.EventUpgrader.upgradeTo
import java.util.*

internal class EventUpgraderTest : StringSpec() {

    init {
        "Test that the upgrade method of an event is executed, and that the upgraded event is returned" {
            data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate
            data class NewEvent(val aggregateId: UUID) : Event<SomeAggregate>
            data class OldEvent(val aggregateId: UUID) : Event<SomeAggregate> {
                override fun upgrade(): Event<SomeAggregate>? = NewEvent(aggregateId)
            }

            val oldEvent = OldEvent(UUID.randomUUID())
            upgradeTo(oldEvent, NewEvent::class).apply {
                aggregateId shouldBe oldEvent.aggregateId
            }
        }
    }
}