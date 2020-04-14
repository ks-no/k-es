package no.ks.kes.lib

import java.util.*


abstract class AggregateRepository {
    abstract fun append(aggregateType: String, aggregateId: UUID, expectedEventNumber: ExpectedEventNumber, events: List<Event<*>>)

    fun <A : Aggregate> read(aggregateId: UUID, aggregateConfiguration: AggregateConfiguration<A>): AggregateReadResult =
        read(
                aggregateId = aggregateId,
                aggregateType = aggregateConfiguration.aggregateType,
                applicator = { s: A?, e -> aggregateConfiguration.applyEvent(e, s) }
        )

    protected abstract fun <A : Aggregate> read(aggregateId: UUID, aggregateType: String, applicator: (state: A?, event: EventWrapper<*>) -> A?): AggregateReadResult
}

sealed class AggregateReadResult{
    data class ExistingAggregate<A: Aggregate>(val aggregateState: A, val eventNumber: Long): AggregateReadResult()
    object NonExistingAggregate: AggregateReadResult()
}

sealed class ExpectedEventNumber() {
    object Any : ExpectedEventNumber()
    object AggregateDoesNotExist : ExpectedEventNumber()
    object AggregateExists : ExpectedEventNumber()
    data class Exact(val eventNumber: Long) : ExpectedEventNumber()
}