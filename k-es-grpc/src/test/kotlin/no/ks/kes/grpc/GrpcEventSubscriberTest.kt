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
import no.ks.kes.lib.EventData
import no.ks.kes.lib.EventWrapper
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

        "onLive should be called before events received if stream is empty" {
            val category = UUID.randomUUID().toString()
            val streamId = "\$ce-$category"
            val eventStoreMock = mockk<EventStoreDBClient>(relaxed = true) {
                every {
                    hint(CompletableFuture::class)
                    subscribeToStream(streamId, any(), any())
                } returns CompletableFuture.completedFuture(mockk<Subscription>(relaxed = true))
            }
            val subscriberFactory = GrpcEventSubscriberFactory(
                eventStoreDBClient = eventStoreMock,
                category = category,
                serdes = mockk()
            )
            var onLiveCalled = 0
            subscriberFactory.createSubscriber("subscriber",
                fromEvent = 0,
                onEvent = { },
                onLive = { onLiveCalled += 1 })
            onLiveCalled shouldBe 1
        }

        "onLive should be called before events received if we start at the last event" {
            val category = UUID.randomUUID().toString()
            val streamId = "\$ce-$category"
            val eventStoreMock = mockk<EventStoreDBClient>(relaxed = true) {
                every {
                    hint(CompletableFuture::class)
                    readStream(streamId, 1, ReadStreamOptions.get().backwards().fromEnd().notResolveLinkTos())
                } returns mockk<CompletableFuture<ReadResult>>(relaxed = true) {
                    every {
                        get().events.first().originalEvent.streamRevision.valueUnsigned
                    } returns 42
                }

                every {
                    hint(CompletableFuture::class)
                    subscribeToStream(streamId, any(), any())
                } returns CompletableFuture.completedFuture(mockk<Subscription>(relaxed = true))
            }
            val subscriberFactory = GrpcEventSubscriberFactory(
                eventStoreDBClient = eventStoreMock,
                category = category,
                serdes = mockk()
            )
            var onLiveCalled = 0
            subscriberFactory.createSubscriber("subscriber",
                fromEvent = 42,
                onEvent = { },
                onLive = { onLiveCalled += 1 })
            onLiveCalled shouldBe 1
        }

        "onLive should be called when subscription receives last event of stream" {
            val category = UUID.randomUUID().toString()
            val streamId = "\$ce-$category"
            val lastEvent = 42L
            val subscribeFrom = 41L
            val listener = slot<SubscriptionListener>()
            val eventStoreMock = mockk<EventStoreDBClient>(relaxed = true) {
                every {
                    hint(CompletableFuture::class)
                    readStream(streamId, 1, match { it.direction == Direction.Backwards })
                } returns mockk<CompletableFuture<ReadResult>>(relaxed = true) {
                    every {
                        get().events.first().originalEvent.streamRevision.valueUnsigned
                    } returns lastEvent
                }

                every {
                    hint(CompletableFuture::class)
                    subscribeToStream(streamId, capture(listener), match { it.startingRevision.valueUnsigned == subscribeFrom })
                } returns CompletableFuture.completedFuture(mockk<Subscription>(relaxed = true))
            }
            val subscriberFactory = GrpcEventSubscriberFactory(
                eventStoreDBClient = eventStoreMock,
                category = category,
                serdes = mockk(relaxed = true) {
                    every {
                        deserialize(any(), any())
                    } returns mockk() { every { upgrade() } returns null }
                }
            )
            var onLiveCalled = 0
            var onEventCalled = 0
            subscriberFactory.createSubscriber("subscriber",
                fromEvent = subscribeFrom,
                onEvent = { onEventCalled += 1},
                onLive = { onLiveCalled += 1 })

            verify { eventStoreMock.readStream(streamId, 1, match { option -> option.direction == Direction.Backwards }) }

            onLiveCalled shouldBe 0
            onEventCalled shouldBe 0

            listener.captured.onEvent(mockk(), eventMock(streamId, 41L))
            listener.captured.onEvent(mockk(), eventMock(streamId,42L))

            onLiveCalled shouldBe 1
            onEventCalled shouldBe 2
        }
    }

    private fun eventMock(streamId: String, eventNumber: Long): ResolvedEvent {
        return mockk<ResolvedEvent>() {
            every { link } returns mockk()
            every { event } returns mockk() {
                every { eventType } returns "eventType"
                every { userMetadata } returns ByteArray(0)
                every { eventData } returns ByteArray(0)
                every { getStreamId() } returns streamId
                every { eventId } returns UUID.randomUUID()
            }
            every { originalEvent.streamRevision.valueUnsigned } returns eventNumber
            every { originalEvent.streamId } returns streamId
        }
    }

}