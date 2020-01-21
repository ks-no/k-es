package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.time.LocalDate
import java.util.*

@SerializationId("Hired")
data class SomethingFailedEvent(
        override val aggregateId: UUID,
        override val timestamp: Instant) : Event<Employee>