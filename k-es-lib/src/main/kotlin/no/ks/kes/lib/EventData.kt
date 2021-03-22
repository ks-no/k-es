package no.ks.kes.lib

import java.util.*

data class EventData (
        val aggregateId : UUID,
        val event: Event<*>,
        val metadata: Metadata? = null,
)