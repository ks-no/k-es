package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event
import no.ks.kes.lib.EventType
import java.time.Instant
import java.time.LocalDate
import java.util.*

@EventType("AddedToPayroll")
data class AddedToPayroll(override val aggregateId: UUID, val startDate: LocalDate, override val timestamp: Instant) : Event<Employee>