package no.ks.kes.sagajdbc.testdomain

import no.ks.kes.sagajdbc.Aggregate
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

        on<StartDateChangedEvent> {
            startDate = it.newStartDate
        }
    }
}