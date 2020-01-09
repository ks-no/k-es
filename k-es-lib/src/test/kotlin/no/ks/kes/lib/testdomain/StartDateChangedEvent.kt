package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event
import no.ks.kes.lib.EventType
import java.time.Instant
import java.time.LocalDate
import java.util.*

@EventType("StartDatePushedBack")
data class StartDateChangedEvent(override val aggregateId: UUID, val newStartDate: LocalDate, override val timestamp: Instant) : Event<Employee>