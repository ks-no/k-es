package no.ks.kes.sagajdbc

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.ks.kes.sagajdbc.AnnotationUtil.getEventType
import no.ks.kes.sagajdbc.testdomain.HiredEvent

internal class EventUtilTest : StringSpec() {

    init {
        "Test that the event type is correctly retrieved from the event annotation" {
            getEventType(HiredEvent::class) shouldBe "Hired"
        }
    }
}