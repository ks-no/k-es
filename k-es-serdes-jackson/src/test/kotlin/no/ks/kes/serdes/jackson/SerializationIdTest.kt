package no.ks.kes.serdes.jackson

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.EventData
import no.ks.kes.lib.SerializationId
import no.ks.kes.lib.getSerializationIdAnnotationValue
import java.util.*

internal class SerializationIdTest : StringSpec() {

    private data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    init {
        "Test that the event type is correctly retrieved from the event annotation" {
            @SerializationId("some-id")
            data class SomeEventData(val aggregateId: UUID) : EventData<SomeAggregate>

            getSerializationIdAnnotationValue(SomeEventData::class) shouldBe "some-id"
        }
    }
}