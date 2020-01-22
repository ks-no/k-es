package no.ks.kes.lib

import java.util.*


interface EventWriter {
    fun write(aggregateType: String, aggregateId: UUID, expectedEventNumber: ExpectedEventNumber, events: List<Event<*>>)
}

sealed class ExpectedEventNumber() {
    object Any : ExpectedEventNumber()
    object AggregateDoesNotExist : ExpectedEventNumber()
    object AggregateExists : ExpectedEventNumber()
    data class Exact(val eventNumber: Long) : ExpectedEventNumber()
}