package no.ks.kes.esjc.testdomain

import no.ks.kes.lib.EventType
import java.time.LocalDate
import java.util.*

@EventType("StartDateChangedEvent")
data class StartDateChangedEvent(override val aggregateId: UUID, val newStartDate: LocalDate) : EmployeeEventType()