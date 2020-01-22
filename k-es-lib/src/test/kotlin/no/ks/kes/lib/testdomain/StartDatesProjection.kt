package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Projection
import java.time.LocalDate
import java.util.*

class StartDatesProjection : Projection() {
    private val startDates: MutableMap<UUID, LocalDate> = HashMap()

    fun getStartDate(aggregateId: UUID?): LocalDate? {
        return startDates[aggregateId]
    }

    init {
        on<Hired> { startDates.put(it.aggregateId, it.startDate) }
    }
}