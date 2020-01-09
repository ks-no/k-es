package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event
import java.time.Instant
import java.time.LocalDate
import java.util.*

data class HireCmd(override val aggregateId: UUID, val startDate: LocalDate) : EmployeeCmd() {
    override fun execute(aggregate: Employee): List<Event<Employee>> =
            if (aggregate.aggregateId != null)
                throw IllegalStateException("The employee has already been created!")
            else
                listOf(HiredEvent(
                        aggregateId = aggregateId,
                        startDate = startDate,
                        timestamp = Instant.now())
                )
}