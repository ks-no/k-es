package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.ks.kes.lib.AnnotationUtil.getSerializationId
import no.ks.kes.lib.testdomain.HiredEvent

internal class EventUtilTest : StringSpec() {

    init {
        "Test that the event type is correctly retrieved from the event annotation" {
            getSerializationId(HiredEvent::class) shouldBe "Hired"
        }
    }
}