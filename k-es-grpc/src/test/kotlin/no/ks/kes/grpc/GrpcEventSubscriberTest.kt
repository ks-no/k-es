package no.ks.kes.grpc

import com.eventstore.dbclient.*
import io.kotest.assertions.throwables.shouldThrowExactly
import io.kotest.core.spec.style.StringSpec
import io.kotest.data.row
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import io.kotest.property.Arb
import io.kotest.property.arbitrary.long
import io.kotest.property.forAll
import io.mockk.*
import org.springframework.util.ReflectionUtils
import java.util.*
import java.util.concurrent.CompletableFuture

internal class GrpcEventSubscriberTest : StringSpec() {
    init {
        "Test that we correctly generate event subscriptions" {
            forAll(Arb.long(-1L..1000L), Arb.long(0L..1000L)) { hwm, eventnumber ->
                val category = UUID.randomUUID().toString()

                val eventStoreMock = mockk<EventStoreDBClient>(relaxed = true) {
                    every { readStream(any(), any(), )} returns CompletableFuture.completedFuture(mockk { every { events } returns emptyList() })
                    every { subscribeToStream(any(), any(), any()) } returns mockk() { every { get() } returns mockk() { every { subscriptionId } returns UUID.randomUUID().toString()} }
                }

                GrpcEventSubscriberFactory(
                        eventStoreDBClient = eventStoreMock,
                        category = category,
                        serdes = mockk()
                ).createSubscriber(hwmId = "aSubscriber", onEvent = { run {} }, fromEvent = hwm) {}
                verify(exactly = 1) { eventStoreMock.subscribeToStream("\$ce-$category", any(), any()) }
                true
            }

        }

        "On error propagates reason after 10 retries" {
            val category = UUID.randomUUID().toString()
            val subscriptionListener = slot<SubscriptionListener>()
            val subscription: CompletableFuture<Subscription> = CompletableFuture.completedFuture(mockk<Subscription> {
                every { subscriptionId } returns UUID.randomUUID().toString()
            })
            val eventStoreMock = mockk<EventStoreDBClient> {
                every { readStream(any(), any())} returns CompletableFuture.completedFuture(mockk { every { events } returns emptyList() })
                every { subscribeToStream("\$ce-$category", capture(subscriptionListener), any()) } returns subscription
            }
            var catchedException: Exception? = null
            GrpcEventSubscriberFactory(
                    eventStoreDBClient = eventStoreMock,
                    category = category,
                    serdes = mockk()
            ).createSubscriber(hwmId = "aSubscriber", onEvent = { run {} }, fromEvent = 1, onError = {
                catchedException = it
            })
            val reason = "connection closed"
            for(i: Int in 0..10) {
                subscriptionListener.captured.onError(
                    subscription.get(),
                    RuntimeException("Consumer too slow to handle event while live")
                )
            }
            (catchedException!! as GrpcSubscriptionException).run {
                reason shouldBe reason
                message shouldBe "Subscription failed. Cause: ${GrpcSubscriptionCause.Unknown}"
                cause should beInstanceOf<RuntimeException>()
            }
            verify(exactly = 11) { eventStoreMock.subscribeToStream("\$ce-$category", any(), any()) }
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
                ).createSubscriber(hwmId = "aSubscriber", onEvent = { run {} }, fromEvent = hwm) {}
            }.message shouldBe "the from-event $hwm is invalid, must be a number equal to or larger than -1"
        }

        "Create event subscriptions using different borderline highwater marks" {
            io.kotest.data.forAll(
                row(-1L, StreamPosition.start()),
                row(0L, StreamPosition.position(0L)),
                row(1L, StreamPosition.position(1L)),
                row(37999L, StreamPosition.position(37999L)),
                row(Long.MAX_VALUE, StreamPosition.position(Long.MAX_VALUE)))
            { hwm, revision ->
                val category = UUID.randomUUID().toString()
                val streamName = "\$ce-$category"

                val eventStoreMock = mockk<EventStoreDBClient> {
                    every { readStream(any(), any())} returns CompletableFuture.completedFuture(mockk { every { events } returns emptyList() })
                    every { subscribeToStream(any(), any(), any()) } returns mockk() { every { get() } returns mockk() { every { subscriptionId } returns UUID.randomUUID().toString()} }
                }

                GrpcEventSubscriberFactory(
                        eventStoreDBClient = eventStoreMock,
                        category = category,
                        serdes = mockk()
                ).createSubscriber(hwmId = "aSubscriber", onEvent = { run {} }, fromEvent = hwm) {}
                verify(exactly = 1) { eventStoreMock.subscribeToStream(
                    "\$ce-$category",
                    any(),
                    withArg {
                        val field = ReflectionUtils.findField(SubscribeToStreamOptions::class.java, "startRevision")!!
                        field.isAccessible = true
                        val subscribeRevision = field.get(it) as StreamPosition<*>
                        subscribeRevision.isStart shouldBe revision.isStart
                        subscribeRevision.isEnd shouldBe revision.isEnd
                        subscribeRevision.position shouldBe revision.position
                    })
                }
            }
        }

        "onLive should be called before events received if stream is empty" {
            val category = UUID.randomUUID().toString()
            val streamId = "\$ce-$category"
            val eventStoreMock = mockk<EventStoreDBClient>(relaxed = true) {
                every { readStream(any(), any())} returns CompletableFuture.completedFuture(mockk { every { events } returns emptyList() })
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
                every { readStream(any(), any())} returns CompletableFuture.completedFuture(mockk { every { events } returns emptyList() })
                every {
                    hint(CompletableFuture::class)
                    readStream(streamId, ReadStreamOptions.get().maxCount(1).backwards().fromEnd().notResolveLinkTos())
                } returns mockk<CompletableFuture<ReadResult>>(relaxed = true) {
                    every {
                        get().events.first().originalEvent.revision
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
                    readStream(streamId, any())
                } returns mockk<CompletableFuture<ReadResult>>(relaxed = true) {
                    every {
                        get().events.first().originalEvent.revision
                    } returns lastEvent
                }

                every {
                    hint(CompletableFuture::class)
                    subscribeToStream(streamId, capture(listener), any())
                } returns CompletableFuture.completedFuture(mockk<Subscription>(relaxed = true))
            }
            val subscriberFactory = GrpcEventSubscriberFactory(
                eventStoreDBClient = eventStoreMock,
                category = category,
                serdes = mockk(relaxed = true) {
                    every {
                        deserialize(any(), any())
                    } returns mockk { every { upgrade() } returns null }
                }
            )
            var onLiveCalled = 0
            var onEventCalled = 0
            subscriberFactory.createSubscriber("subscriber",
                fromEvent = subscribeFrom,
                onEvent = { onEventCalled += 1},
                onLive = { onLiveCalled += 1 })

            verify { eventStoreMock.readStream(streamId, any()) }

            onLiveCalled shouldBe 0
            onEventCalled shouldBe 0

            listener.captured.onEvent(mockk(), eventMock(streamId, 41L))
            listener.captured.onEvent(mockk(), eventMock(streamId,42L))

            onLiveCalled shouldBe 1
            onEventCalled shouldBe 2
        }
    }

    private fun eventMock(streamId: String, eventNumber: Long): ResolvedEvent {
        return mockk() {
            every { link } returns mockk()
            every { event } returns mockk {
                every { eventType } returns "eventType"
                every { userMetadata } returns ByteArray(0)
                every { eventData } returns ByteArray(0)
                every { getStreamId() } returns streamId
                every { eventId } returns UUID.randomUUID()
            }
            every { originalEvent.revision } returns eventNumber
            every { originalEvent.streamId } returns streamId
        }
    }

}