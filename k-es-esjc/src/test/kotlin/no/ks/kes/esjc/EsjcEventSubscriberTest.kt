package no.ks.kes.esjc

import com.github.msemys.esjc.CatchUpSubscriptionListener
import com.github.msemys.esjc.EventStore
import io.kotlintest.matchers.instanceOf
import io.kotlintest.specs.StringSpec
import io.mockk.mockk
import io.mockk.verify
import java.util.*
import java.util.concurrent.ThreadLocalRandom

internal class EsjcEventSubscriberTest : StringSpec() {
    init {
        "Test that we correctly generate event subscriptions" {
            val hwm = ThreadLocalRandom.current().nextLong()
            val category = UUID.randomUUID().toString()

            val eventStoreMock = mockk<EventStore>(relaxed = true)

            EsjcEventSubscriber(
                    eventStore = eventStoreMock,
                    fromEvent = hwm,
                    category = category,
                    serdes = mockk()
            ).subscribe { run {} }
            verify(exactly = 1) { eventStoreMock.subscribeToStreamFrom("\$ce-$category", eq(hwm), any(), ofType<CatchUpSubscriptionListener>()) }
        }
    }
}