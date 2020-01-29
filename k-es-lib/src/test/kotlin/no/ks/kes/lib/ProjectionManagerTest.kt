package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.invoke
import io.mockk.mockk
import io.mockk.slot
import no.ks.kes.lib.testdomain.Hired
import java.time.Instant
import java.time.LocalDate
import java.util.*

internal class ProjectionManagerTest : StringSpec() {

    init {
        "test that a projection can handle incoming events and mutate its state accordingly" {
            val startDates = object: Projection() {
                private val startDates: MutableMap<UUID, LocalDate> = HashMap()

                fun getStartDate(aggregateId: UUID?): LocalDate? {
                    return startDates[aggregateId]
                }

                init {
                    on<Hired> { startDates.put(it.aggregateId, it.startDate) }
                }
            }

            val slot = slot<(EventWrapper<*>) -> Unit>()

            ProjectionManager(
                    eventSubscriber = mockk<EventSubscriber>()
                            .apply { every { addSubscriber(
                                    consumerName = "ProjectionManager",
                                    fromEvent = 0L,
                                    onEvent = capture(slot),
                                    onClose = any(),
                                    onLive = any()
                            ) } returns Unit},
                    projections = setOf(startDates),
                    fromEvent = 0L,
                    hwmUpdater = {},
                    onClose = {}
            )

            val hiredEvent = Hired(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now(),
                    timestamp = Instant.now(),
                    recruitedBy = UUID.randomUUID())

            //when we invoke the captured handler from the manager with the subscribed event
            slot.invoke(EventWrapper(hiredEvent, 0))

            //the projection should update
            startDates.getStartDate(hiredEvent.aggregateId)!! shouldBe hiredEvent.startDate
        }
    }
}