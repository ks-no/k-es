package no.ks.kes.esjc.testdomain

import no.ks.kes.lib.Cmd

abstract class EmployeeCmd : Cmd<EmployeeEventType, Employee> {
    override fun initAggregate(): Employee = Employee()
}

