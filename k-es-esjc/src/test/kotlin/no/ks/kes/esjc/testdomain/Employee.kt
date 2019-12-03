package no.ks.kes.esjc.testdomain

import no.ks.kes.lib.Aggregate
import java.time.LocalDate
import java.util.*

class Employee() : Aggregate<EmployeeEventType>() {
    override val aggregateType = "employee"

    var aggregateId: UUID? = null
    var startDate: LocalDate? = null

    init {
        HiredEvent::class then {
            aggregateId = it.aggregateId
            startDate = it.startDate
        }

        StartDateChangedEvent::class then {
            startDate = it.newStartDate
        }
    }
}