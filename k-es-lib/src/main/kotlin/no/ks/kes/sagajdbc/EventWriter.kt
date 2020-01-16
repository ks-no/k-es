package no.ks.kes.sagajdbc

import java.util.*


interface EventWriter {
    fun write(aggregateType: String, aggregateId: UUID, expectedEventNumber: Long, events: List<Event<*>>, useOptimisticLocking: Boolean)
}