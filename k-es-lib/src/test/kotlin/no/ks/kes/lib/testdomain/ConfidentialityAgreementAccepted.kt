package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event
import no.ks.kes.lib.EventType
import java.time.Instant
import java.util.*

@EventType("AddedToPayroll")
data class ConfidentialityAgreementAccepted(
        override val aggregateId: UUID,
        override val timestamp: Instant) : Event<Employee>