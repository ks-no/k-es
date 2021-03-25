package no.ks.kes.lib

import java.util.*

data class Event (
        val aggregateId : UUID,
        val eventData: EventData<out Aggregate>,
        val metadata: Metadata? = null,
)