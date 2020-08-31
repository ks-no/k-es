package no.ks.kes.esjc

import com.github.msemys.esjc.CatchUpSubscriptionListener
import com.github.msemys.esjc.EventStore
import io.kotest.core.spec.style.StringSpec
import io.kotest.data.forAll
import io.kotest.data.row
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.util.*

internal class EsjcEventSubscriberTest : StringSpec() {
    init {
        "Test that we correctly generate event subscriptions" {
            forAll<Long, Long> { hwm, eventnumber ->
                val category = UUID.randomUUID().toString()

                val eventStoreMock = mockk<EventStore>(relaxed = true)

                EsjcEventSubscriberFactory(
                        eventStore = eventStoreMock,
                        category = category,
                        serdes = mockk()
                ).createSubscriber(subscriber = "aSubscriber", onEvent = { run {} }, fromEvent = hwm)
                verify(exactly = 1) { eventStoreMock.subscribeToStreamFrom("\$ce-$category", eq(eventnumber), any(), ofType<CatchUpSubscriptionListener>()) }
            }

        }

        "Create event subscriptions using different borderline highwater marks" {
            forAll(row(-1L, null), row(0L, 0L), row(1L, 1L), row(37999L, 37999L), row(Long.MAX_VALUE, Long.MAX_VALUE)) { hwm, eventnumber ->
                val category = UUID.randomUUID().toString()
                val streamName = "\$ce-$category"

                val eventStoreMock = mockk<EventStore> {
                    every { subscribeToStreamFrom(streamName, eventnumber, any(), ofType<CatchUpSubscriptionListener>()) } returns mockk()
                }

                EsjcEventSubscriberFactory(
                        eventStore = eventStoreMock,
                        category = category,
                        serdes = mockk()
                ).createSubscriber(subscriber = "aSubscriber", onEvent = { run {} }, fromEvent = hwm)
                verify(exactly = 1) { eventStoreMock.subscribeToStreamFrom("\$ce-$category", eventnumber, any(), ofType<CatchUpSubscriptionListener>()) }
            }
        }
    }
}