package no.ks.kes.serdes.jackson

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.Event
import no.ks.kes.lib.SerializationId
import no.ks.kes.lib.getSerializationIdAnnotationValue
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