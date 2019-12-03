package no.ks.kes.lib

import java.util.*


interface EventWriter {
    fun write(aggregateType: String, aggregateId: UUID, expectedEventNumber: Long, events: List<Event>, useOptimisticLocking: Boolean)
}