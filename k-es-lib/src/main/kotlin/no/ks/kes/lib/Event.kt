package no.ks.kes.lib

import java.util.*

data class Event<E : EventData<*>> (
        val aggregateId : UUID,
        val eventData: E,
        val metadata: Metadata? = null,
)