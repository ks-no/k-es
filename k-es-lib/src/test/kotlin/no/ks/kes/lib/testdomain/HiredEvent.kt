package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.time.LocalDate
import java.util.*

@SerializationId("Hired")
data class HiredEvent(
        override val aggregateId: UUID,
        val recruitedBy: UUID,
        val startDate: LocalDate,
        override val timestamp: Instant) : Event<Employee>