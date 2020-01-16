package no.ks.kes.esjc.testdomain

import no.ks.kes.sagajdbc.Event
import no.ks.kes.sagajdbc.EventType
import java.time.Instant
import java.time.LocalDate
import java.util.*

@EventType("Hired")
data class HiredEvent(override val aggregateId: UUID, val startDate: LocalDate, override val timestamp: Instant) : Event<Employee>