package no.ks.kes.esjc.testdomain

import no.ks.kes.sagajdbc.Cmd

abstract class EmployeeCmd : Cmd<Employee> {
    override fun initAggregate(): Employee = Employee()
}

