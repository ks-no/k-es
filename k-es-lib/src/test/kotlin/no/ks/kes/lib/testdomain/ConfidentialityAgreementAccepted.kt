package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Event
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.util.*

@SerializationId("AddedToPayroll")
data class ConfidentialityAgreementAccepted(
        override val aggregateId: UUID,
        override val timestamp: Instant) : Event<Employee>