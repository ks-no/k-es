package no.ks.kes.lib

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import java.util.*

internal class ProjectionTest : StringSpec() {

    init {
        "test that a projection can handle incoming events and mutate its state accordingly" {
            data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            data class Hired(
                    val aggregateId: UUID,
                    val recruitedBy: UUID,
                    val startDate: LocalDate) : EventData<SomeAggregate>

            class StartDatesProjection : Projection() {
                private val startDates: MutableMap<UUID, LocalDate> = HashMap()

                fun getStartDate(aggregateId: UUID?): LocalDate? {
                    return startDates[aggregateId]
                }

                init {
                    on<Hired> { startDates.put(it.aggregateId, it.event.startDate) }
                }
            }

            val aggregateId = UUID.randomUUID()
            val hiredEvent = Hired(
                    aggregateId = aggregateId,
                    startDate = LocalDate.now(),
                    recruitedBy = UUID.randomUUID())

            StartDatesProjection().apply {
                getConfiguration { it.simpleName!! }.accept(EventWrapper(aggregateId, hiredEvent, null, 0, hiredEvent::class.simpleName!!))

                getStartDate(hiredEvent.aggregateId) shouldBe LocalDate.now()
            }
        }
    }
}