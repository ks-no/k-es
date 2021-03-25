package no.ks.kes.lib

import java.util.*

data class EventWrapper<T : EventData<*>>(
        val aggregateId: UUID,
        val event: T,
        val metadata: Metadata? = null,
        val eventNumber: Long,
        val serializationId: String
)