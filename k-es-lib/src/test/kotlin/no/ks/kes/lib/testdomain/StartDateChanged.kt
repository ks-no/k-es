package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.time.LocalDate
import java.util.*

@SerializationId("StartDatePushedBack")
data class StartDateChanged(override val aggregateId: UUID, val newStartDate: LocalDate, override val timestamp: Instant) : Event<Employee>