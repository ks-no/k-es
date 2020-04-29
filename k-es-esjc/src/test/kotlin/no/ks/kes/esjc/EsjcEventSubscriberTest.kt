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
            val hwm = ThreadLocalRandom.current().nextLong(-1, 10000)
            val category = UUID.randomUUID().toString()

            val eventStoreMock = mockk<EventStore>(relaxed = true)

            EsjcEventSubscriberFactory(
                    eventStore = eventStoreMock,
                    category = category,
                    serdes = mockk()
            ).createSubscriber(subscriber = "aSubscriber", onEvent =  { run {} }, fromEvent = hwm)
            verify(exactly = 1) { eventStoreMock.subscribeToStreamFrom("\$ce-$category", eq(hwm), any(), ofType<CatchUpSubscriptionListener>()) }
        }
    }
}