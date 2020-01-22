package no.ks.kes.lib

import java.util.*


interface AggregateRepository {
    fun write(aggregateType: String, aggregateId: UUID, expectedEventNumber: ExpectedEventNumber, events: List<Event<*>>)
    fun <A : Aggregate> read(aggregateId: UUID, aggregate: A): A
}

sealed class ExpectedEventNumber() {
    object Any : ExpectedEventNumber()
    object AggregateDoesNotExist : ExpectedEventNumber()
    object AggregateExists : ExpectedEventNumber()
    data class Exact(val eventNumber: Long) : ExpectedEventNumber()
}