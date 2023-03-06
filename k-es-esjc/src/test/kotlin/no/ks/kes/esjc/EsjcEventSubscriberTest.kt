package no.ks.kes.esjc

import com.github.msemys.esjc.*
import io.kotest.assertions.throwables.shouldThrowExactly
import io.kotest.core.spec.style.StringSpec
import io.kotest.data.forAll
import io.kotest.data.row
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import io.mockk.*
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
                ).createSubscriber(hwmId = "aSubscriber", onEvent = { run {} }, fromEvent = hwm)
                verify(exactly = 1) { eventStoreMock.subscribeToStreamFrom("\$ce-$category", eq(eventnumber), any(), ofType<CatchUpSubscriptionListener>()) }
            }

        }

        "On close propagetes reason" {
            val category = UUID.randomUUID().toString()
            val eventnumber: Long = 1
            val catchUpSubscriptionListener = slot<CatchUpSubscriptionListener>()
            val subscription: CatchUpSubscription = mockk()
            val eventStoreMock = mockk<EventStore> {
                every { subscribeToStreamFrom("\$ce-$category", eventnumber, any(), capture(catchUpSubscriptionListener)) } returns subscription
            }
            var catchedException: Exception? = null
            EsjcEventSubscriberFactory(
                    eventStore = eventStoreMock,
                    category = category,
                    serdes = mockk()
            ).createSubscriber(hwmId = "aSubscriber", onEvent = { run {} }, fromEvent = 1, onClose = {
                catchedException = it
            })
            val subscriptionDropReason = SubscriptionDropReason.ConnectionClosed
            catchUpSubscriptionListener.captured.onClose(subscription, subscriptionDropReason, ConnectionClosedException("Connection was closed"))
            (catchedException!! as EsjcSubscriptionDroppedException).run {
                reason shouldBe subscriptionDropReason
                message shouldBe "Subscription was dropped. Reason: $subscriptionDropReason"
                cause should beInstanceOf<ConnectionClosedException>()
            }
            verify(exactly = 1) { eventStoreMock.subscribeToStreamFrom("\$ce-$category", eq(eventnumber), any(), ofType<CatchUpSubscriptionListener>()) }
            confirmVerified(subscription)

        }

        "Create event subsription starting on MIN_VALUE" {
            val hwm = Long.MIN_VALUE
            val category = UUID.randomUUID().toString()
            val eventStoreMock = mockk<EventStore>(relaxed = true)

            shouldThrowExactly<IllegalStateException> {
                EsjcEventSubscriberFactory(
                        eventStore = eventStoreMock,
                        category = category,
                        serdes = mockk()
                ).createSubscriber(hwmId = "aSubscriber", onEvent = { run {} }, fromEvent = hwm)
            }.message shouldBe "the from-event $hwm is invalid, must be a number equal to or larger than -1"
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
                ).createSubscriber(hwmId = "aSubscriber", onEvent = { run {} }, fromEvent = hwm)
                verify(exactly = 1) { eventStoreMock.subscribeToStreamFrom("\$ce-$category", eventnumber, any(), ofType<CatchUpSubscriptionListener>()) }
            }
        }
    }
}