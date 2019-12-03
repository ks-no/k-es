package no.ks.kes.esjc.testdomain

import no.ks.kes.lib.Projection
import java.time.LocalDate
import java.util.*

class StartDatesProjection : Projection() {
    private val startDates: MutableMap<UUID, LocalDate> = HashMap()

    fun getStartDate(aggregateId: UUID?): LocalDate? {
        return startDates[aggregateId]
    }

    init {
        HiredEvent::class project { startDates.put(it.aggregateId, it.startDate) }
    }
}