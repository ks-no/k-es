package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.invoke
import io.mockk.mockk
import io.mockk.slot
import java.time.Instant
import java.util.*

internal class ProjectionsTest : StringSpec() {

    @SerializationId("some-id")
    data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SagaTest.SomeAggregate>

    init {
        "test that a projection can handle incoming events and mutate its state accordingly" {
            val startDates = object: Projection() {
                private val startDates: MutableSet<UUID> = mutableSetOf()

                fun hasBeenProjected(aggregateId: UUID): Boolean {
                    return startDates.contains(aggregateId)
                }

                init {
                    on<SomeEvent> { startDates.add(it.aggregateId) }
                }
            }

            val slot = slot<(EventWrapper<*>) -> Unit>()
            val consumerName = "ProjectionManager"
            Projections.initialize(
                    eventSubscriberFactory = mockk<EventSubscriberFactory>()
                            .apply { every { createSubscriber(
                                    subscriber = consumerName,
                                    fromEvent = 0L,
                                    onEvent = capture(slot),
                                    onClose = any(),
                                    onLive = any()
                            ) } returns Unit},
                    projections = setOf(startDates),
                    projectionRepository = object: ProjectionRepository {
                        override val hwmTracker =  object: HwmTrackerRepository {
                            override fun getOrInit(subscriber: String): Long = 0L
                            override fun update(subscriber: String, hwm: Long) {}
                        }

                        override fun transactionally(runnable: () -> Unit) {
                            runnable.invoke()
                        }
                    },
                    onClose = {},
                    subscriber = consumerName
            )

            val hiredEvent = SomeEvent(
                    aggregateId = UUID.randomUUID(),
                    timestamp = Instant.now()
            )

            //when we invoke the captured handler from the manager with the subscribed event
            slot.invoke(EventWrapper(hiredEvent, 0))

            //the projection should update
            startDates.hasBeenProjected(hiredEvent.aggregateId) shouldBe true
        }
    }
}