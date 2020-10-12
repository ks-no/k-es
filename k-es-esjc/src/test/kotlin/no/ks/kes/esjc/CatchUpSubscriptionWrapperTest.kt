package no.ks.kes.esjc

import com.github.msemys.esjc.CatchUpSubscription
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verifyAll

class CatchUpSubscriptionWrapperTest : StringSpec() {

    init {
        "isSubscribedToAll" {
            val subscription: CatchUpSubscription = mockk {
                every { isSubscribedToAll } returns true
            }
            CatchUpSubscriptionWrapper(subscription).isSubscribedToAll shouldBe true
            verifyAll { subscription.isSubscribedToAll }
        }

        "lastProcessedEvent" {
            val subscription: CatchUpSubscription = mockk {
                every { lastProcessedEventNumber() } returns 0
            }
            CatchUpSubscriptionWrapper(subscription).lastProcessedEvent() shouldBe 0
            verifyAll { subscription.lastProcessedEventNumber() }
        }
    }
}
