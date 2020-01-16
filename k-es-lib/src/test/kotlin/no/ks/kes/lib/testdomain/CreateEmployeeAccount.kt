package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event
import java.util.*

data class CreateEmployeeAccount(override val aggregateId: UUID): EmployeeCmd() {
    override fun execute(aggregate: Employee): List<Event<Employee>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}
