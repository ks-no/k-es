package no.ks.kes.lib

import java.util.*

data class WriteEventWrapper (
        val aggregateId : UUID,
        val event: Event<*>,
        val metadata: EventMetadata? = null,
)