package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Aggregate
import java.time.LocalDate
import java.util.*


class Employee : Aggregate() {
    override val aggregateType = "employee"

    var aggregateId: UUID? = null
    var startDate: LocalDate? = null

    init {
        on<HiredEvent> {
            aggregateId = it.aggregateId
            startDate = it.startDate
        }

        on<StartDateChanged> {
            startDate = it.newStartDate
        }
    }
}
