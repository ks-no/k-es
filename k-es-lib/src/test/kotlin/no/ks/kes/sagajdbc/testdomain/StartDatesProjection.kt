package no.ks.kes.sagajdbc.testdomain

import no.ks.kes.sagajdbc.Projection
import java.time.LocalDate
import java.util.*

class StartDatesProjection : Projection() {
    private val startDates: MutableMap<UUID, LocalDate> = HashMap()

    fun getStartDate(aggregateId: UUID?): LocalDate? {
        return startDates[aggregateId]
    }

    init {
        on<HiredEvent> { startDates.put(it.aggregateId, it.startDate) }
    }
}