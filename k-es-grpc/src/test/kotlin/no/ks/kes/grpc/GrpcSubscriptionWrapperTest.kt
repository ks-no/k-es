package no.ks.kes.grpc

import com.github.msemys.esjc.CatchUpSubscription
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verifyAll

class GrpcSubscriptionWrapperTest : StringSpec() {

    init {

        "isSubscribedToAll" {
            val subscription: GrpcSubscriptionWrapper = GrpcSubscriptionWrapper("\$all", mockk()) { 0 }
            subscription.isSubscribedToAll shouldBe true
        }

        "lastProcessedEvent" {
            val subscription: GrpcSubscriptionWrapper = GrpcSubscriptionWrapper("stream-id", mockk()) { 42 }
            subscription.lastProcessedEvent() shouldBe 42
        }
    }
}
