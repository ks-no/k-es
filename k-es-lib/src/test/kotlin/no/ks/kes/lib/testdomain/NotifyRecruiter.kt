package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event
import java.util.*

data class NotifyRecruiter(val recruiterId: UUID, val employeeId: UUID): EmployeeCmd() {
    override fun execute(aggregate: Employee): List<Event<Employee>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }


    override val aggregateId: UUID
        get() = employeeId
}
