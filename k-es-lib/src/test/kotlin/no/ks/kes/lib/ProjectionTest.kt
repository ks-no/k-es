package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import java.time.LocalDate
import java.util.*

internal class ProjectionTest : StringSpec() {

    init {
        "test that a projection can handle incoming events and mutate its state accordingly" {
            data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            data class Hired(
                    override val aggregateId: UUID,
                    val recruitedBy: UUID,
                    val startDate: LocalDate) : Event<SomeAggregate>

            class StartDatesProjection : Projection() {
                private val startDates: MutableMap<UUID, LocalDate> = HashMap()

                fun getStartDate(aggregateId: UUID?): LocalDate? {
                    return startDates[aggregateId]
                }

                init {
                    on<Hired> { startDates.put(it.aggregateId, it.startDate) }
                }
            }

            val hiredEvent = Hired(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now(),
                    recruitedBy = UUID.randomUUID())

            StartDatesProjection().apply {
                getConfiguration { it.simpleName!! }.accept(EventWrapper(hiredEvent, 0, hiredEvent::class.simpleName!!))

                getStartDate(hiredEvent.aggregateId) shouldBe LocalDate.now()
            }
        }
    }
}