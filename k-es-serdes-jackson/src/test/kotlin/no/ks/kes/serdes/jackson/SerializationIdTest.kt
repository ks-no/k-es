package no.ks.kes.serdes.jackson

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.Event
import java.time.Instant
import java.util.*

internal class SerializationIdTest : StringSpec() {

    private data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    init {
        "Test that the event type is correctly retrieved from the event annotation" {
            @SerializationId("some-id")
            data class SomeEvent(override val aggregateId: UUID) : Event<SomeAggregate>

            getSerializationIdAnnotationValue(SomeEvent::class) shouldBe "some-id"
        }
    }
}