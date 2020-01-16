package no.ks.kes.sagajdbc.testdomain

import no.ks.kes.sagajdbc.Event
import no.ks.kes.sagajdbc.EventType
import java.time.Instant
import java.time.LocalDate
import java.util.*

@EventType("AddedToPayroll")
data class ConfidentialityAgreementAccepted(
        override val aggregateId: UUID,
        override val timestamp: Instant) : Event<Employee>