package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.ks.kes.lib.AnnotationUtil.getSerializationId
import java.time.Instant
import java.util.*

internal class EventUtilTest : StringSpec() {

    init {
        "Test that the event type is correctly retrieved from the event annotation" {
            @SerializationId("some-id")
            data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SagaTest.SomeAggregate>

            getSerializationId(SomeEvent::class) shouldBe "some-id"
        }
    }
}