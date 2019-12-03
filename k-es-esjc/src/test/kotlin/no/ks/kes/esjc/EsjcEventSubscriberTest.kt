package no.ks.kes.esjc

import com.github.msemys.esjc.CatchUpSubscriptionListener
import com.github.msemys.esjc.EventStore
import io.kotlintest.specs.StringSpec
import io.mockk.mockk
import io.mockk.verify
import java.util.*
import java.util.concurrent.ThreadLocalRandom

internal class EsjcEventSubscriberTest : StringSpec() {
    init {
        "Test that we correctly generate event subscriptions" {
            val hwm = ThreadLocalRandom.current().nextLong()
            val listener = mockk<CatchUpSubscriptionListener>()
            val category = UUID.randomUUID().toString()

            val eventStoreMock = mockk<EventStore>(relaxed = true)

            EsjcEventSubscriber(eventStoreMock).subscribeByCategory(category, hwm, listener)
            verify(exactly = 1) { eventStoreMock.subscribeToStreamFrom("\$ce-$category", eq(hwm), any(), eq(listener)) }
        }
    }
}