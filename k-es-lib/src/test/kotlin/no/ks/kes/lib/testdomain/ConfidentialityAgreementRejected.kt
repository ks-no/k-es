package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event
import no.ks.kes.lib.EventType
import java.time.Instant
import java.time.LocalDate
import java.util.*

@EventType("AddedToPayroll")
data class ConfidentialityAgreementRejected(
        override val aggregateId: UUID,
        val respondByDate: LocalDate,
        override val timestamp: Instant) : Event<Employee>