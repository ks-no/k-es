package no.ks.kes.grpc

import com.eventstore.dbclient.*
import com.eventstore.dbclient.Subscription
import com.eventstore.dbclient.SubscriptionListener
import io.kotest.assertions.throwables.shouldThrowExactly
import io.kotest.core.spec.style.StringSpec
import io.kotest.data.forAll
import io.kotest.data.row
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import io.mockk.*
import no.ks.kes.grpc.GrpcSubscriptionDroppedReason.ConnectionShutDown
import java.util.*
import java.util.concurrent.CompletableFuture

internal class GrpcEventSubscriberTest : StringSpec() {
    init {
        "Test that we correctly generate event subscriptions" {
            forAll<Long, Long> { hwm, eventnumber ->
                val category = UUID.randomUUID().toString()

                val eventStoreMock = mockk<EventStoreDBClient>(relaxed = true)

                GrpcEventSubscriberFactory(
                        eventStoreDBClient = eventStoreMock,
                        category = category,
                        serdes = mockk()
                ).createSubscriber(subscriber = "aSubscriber", onEvent = { run {} }, fromEvent = hwm)
                verify(exactly = 1) { eventStoreMock.subscribeToStream("\$ce-$category", any(), SubscribeToStreamOptions.get().resolveLinkTos().fromRevision(eventnumber)) }
            }

        }

        "On close propagates reason" {
            val category = UUID.randomUUID().toString()
            val eventnumber: Long = 1
            val subscriptionListener = slot<SubscriptionListener>()
            val subscription: CompletableFuture<Subscription> = CompletableFuture.completedFuture(mockk<Subscription> {
                every { subscriptionId } returns UUID.randomUUID().toString()
            })
            val eventStoreMock = mockk<EventStoreDBClient> {
                every { subscribeToStream("\$ce-$category", capture(subscriptionListener), any()) } returns subscription
            }
            var catchedException: Exception? = null
            GrpcEventSubscriberFactory(
                    eventStoreDBClient = eventStoreMock,
                    category = category,
                    serdes = mockk()
            ).createSubscriber(subscriber = "aSubscriber", onEvent = { run {} }, fromEvent = 1, onClose = {
                catchedException = it
            })
            val reason = "connection closed"
            subscriptionListener.captured.onError(subscription.get(), ConnectionShutdownException())
            (catchedException!! as GrpcSubscriptionDroppedException).run {
                reason shouldBe reason
                message shouldBe "Subscription was dropped. Reason: $ConnectionShutDown"
                cause should beInstanceOf<ConnectionShutdownException>()
            }
            verify(exactly = 1) { eventStoreMock.subscribeToStream("\$ce-$category", any(), any()) }
            verify(exactly = 1) { subscription.get().subscriptionId }
            confirmVerified(subscription.get())
        }

        "Create event subsription starting on MIN_VALUE" {
            val hwm = Long.MIN_VALUE
            val category = UUID.randomUUID().toString()
            val eventStoreMock = mockk<EventStoreDBClient>(relaxed = true)

            shouldThrowExactly<IllegalStateException> {
                GrpcEventSubscriberFactory(
                        eventStoreDBClient = eventStoreMock,
                        category = category,
                        serdes = mockk()
                ).createSubscriber(subscriber = "aSubscriber", onEvent = { run {} }, fromEvent = hwm)
            }.message shouldBe "the from-event $hwm is invalid, must be a number equal to or larger than -1"
        }

        "Create event subscriptions using different borderline highwater marks" {
            forAll(
                row(-1L, StreamRevision.START),
                row(0L, StreamRevision(0L)),
                row(1L, StreamRevision(1L)),
                row(37999L, StreamRevision(37999L)),
                row(Long.MAX_VALUE, StreamRevision(Long.MAX_VALUE)))
            { hwm, revision ->
                val category = UUID.randomUUID().toString()
                val streamName = "\$ce-$category"

                val eventStoreMock = mockk<EventStoreDBClient> {
                    every { subscribeToStream(streamName, any(), any()) } returns CompletableFuture.completedFuture(mockk())
                }

                GrpcEventSubscriberFactory(
                        eventStoreDBClient = eventStoreMock,
                        category = category,
                        serdes = mockk()
                ).createSubscriber(subscriber = "aSubscriber", onEvent = { run {} }, fromEvent = hwm)
                verify(exactly = 1) { eventStoreMock.subscribeToStream("\$ce-$category", any(), withArg { it.startingRevision shouldBe revision }) }
            }
        }

    }

}