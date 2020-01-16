package no.ks.kes.sagajdbc.testdomain

import no.ks.kes.sagajdbc.Event
import java.util.*

data class SendConfidentialityAgreement(override val aggregateId: UUID): EmployeeCmd() {
    override fun execute(aggregate: Employee): List<Event<Employee>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}
