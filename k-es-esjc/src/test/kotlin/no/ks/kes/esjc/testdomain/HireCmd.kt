package no.ks.kes.esjc.testdomain

import java.time.LocalDate
import java.util.*

data class HireCmd(override val aggregateId: UUID, val startDate: LocalDate) : EmployeeCmd() {
    override fun execute(aggregate: Employee): List<EmployeeEventType> =
            if (aggregate.aggregateId != null)
                throw IllegalStateException("The employee has already been created!")
            else
                listOf(HiredEvent(
                        aggregateId = aggregateId,
                        startDate = startDate)
                )
}