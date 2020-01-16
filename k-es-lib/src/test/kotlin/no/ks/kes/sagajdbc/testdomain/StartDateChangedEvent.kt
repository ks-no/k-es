package no.ks.kes.sagajdbc.testdomain

import no.ks.kes.sagajdbc.Event
import no.ks.kes.sagajdbc.EventType
import java.time.Instant
import java.time.LocalDate
import java.util.*

@EventType("StartDatePushedBack")
data class StartDateChangedEvent(override val aggregateId: UUID, val newStartDate: LocalDate, override val timestamp: Instant) : Event<Employee>