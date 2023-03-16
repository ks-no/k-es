package no.ks.kes.grpc

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class GrpcSubscriptionWrapperTest : StringSpec() {

    init {

        "isSubscribedToAll" {
            val subscription: GrpcSubscriptionWrapper = GrpcSubscriptionWrapper("\$all" ) { 0 }
            subscription.isSubscribedToAll shouldBe true
        }

        "lastProcessedEvent" {
            val subscription: GrpcSubscriptionWrapper = GrpcSubscriptionWrapper("stream-id") { 42 }
            subscription.lastProcessedEvent() shouldBe 42
        }
    }
}
