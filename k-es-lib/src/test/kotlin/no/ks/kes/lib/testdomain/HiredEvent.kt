package no.ks.kes.lib.testdomain

import no.ks.kes.lib.EventType
import java.time.LocalDate
import java.util.*

@EventType("Hired")
data class HiredEvent(override val aggregateId: UUID, val startDate: LocalDate) : EmployeeEventType()