package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event


abstract class EmployeeEventType : Event {
    override val timestamp = 0L
}