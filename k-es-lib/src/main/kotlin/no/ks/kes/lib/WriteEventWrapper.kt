package no.ks.kes.lib

import java.util.*

data class WriteEventWrapper<T : Event<*>>(
        val aggregateId : UUID,
        val event: T,
        val metadata: EventMetadata,
)