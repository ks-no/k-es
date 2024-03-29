package no.ks.kes.lib

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.invoke
import io.mockk.mockk
import io.mockk.slot
import java.util.*
import kotlin.reflect.KClass

internal class ProjectionsTest : StringSpec() {

    private data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    data class SomeEventData(val aggregateId: UUID) : EventData<SomeAggregate>

    init {
        "test that a projection can handle incoming events and mutate its state accordingly" {
            val startDates = object : Projection() {
                private val startDates: MutableSet<UUID> = mutableSetOf()

                fun hasBeenProjected(aggregateId: UUID): Boolean {
                    return startDates.contains(aggregateId)
                }

                init {
                    on<SomeEventData> { startDates.add(it.aggregateId) }
                }
            }

            val slot = slot<(EventWrapper<*>) -> Unit>()
            val consumerName = "ProjectionManager"
            Projections.initialize(
                    eventSubscriberFactory = mockk<EventSubscriberFactory<SimpleEventSubscription>> {
                        every { createSubscriber(
                                hwmId = consumerName,
                                fromEvent = 0L,
                                onEvent = capture(slot),
                                onError = any(),
                                onLive = any()
                        ) } returns SimpleEventSubscription(-1)
                        every { getSerializationId(any()) } answers { firstArg<KClass<EventData<*>>>().simpleName!! }
                    },
                    projections = setOf(startDates),
                    projectionRepository = object : ProjectionRepository {
                        override val hwmTracker = object : HwmTrackerRepository {
                            override fun current(subscriber: String): Long? = 0L
                            override fun getOrInit(subscriber: String): Long = 0L
                            override fun update(subscriber: String, hwm: Long) {}
                        }

                        override fun transactionally(runnable: () -> Unit) {
                            runnable.invoke()
                        }
                    },
                    onError = {},
                    hwmId = consumerName
            )

            val aggregateId = UUID.randomUUID()
            val hiredEvent = SomeEventData(
                    aggregateId = aggregateId
            )

            //when we invoke the captured handler from the manager with the subscribed event
            slot.invoke(EventWrapper(Event(aggregateId,hiredEvent,null), 0, hiredEvent::class.simpleName!!))

            //the projection should update
            startDates.hasBeenProjected(hiredEvent.aggregateId) shouldBe true
        }
    }
}

internal class SimpleEventSubscription(val lastProcessed: Long): EventSubscription {
    override fun lastProcessedEvent(): Long = lastProcessed
}