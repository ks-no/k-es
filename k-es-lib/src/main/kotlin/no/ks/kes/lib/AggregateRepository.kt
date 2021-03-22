package no.ks.kes.lib

import java.util.*
import kotlin.reflect.KClass


abstract class AggregateRepository() {
    abstract fun append(aggregateType: String, aggregateId: UUID, expectedEventNumber: ExpectedEventNumber, events: List<EventData>)
    abstract fun getSerializationId(eventClass: KClass<Event<*>>): String

    fun <A : Aggregate> read(aggregateId: UUID, aggregateConfiguration: ValidatedAggregateConfiguration<A>): AggregateReadResult =
            read(
                    aggregateId = aggregateId,
                    aggregateType = aggregateConfiguration.aggregateType,
                    applicator = { s: A?, e -> aggregateConfiguration.applyEvent(e, s) }
            )

    protected abstract fun <A : Aggregate> read(aggregateId: UUID, aggregateType: String, applicator: (state: A?, event: EventWrapper<*>) -> A?): AggregateReadResult
}

sealed class AggregateReadResult{
    data class InitializedAggregate<A: Aggregate>(val aggregateState: A, val eventNumber: Long): AggregateReadResult()
    data class UninitializedAggregate(val eventNumber: Long): AggregateReadResult()
    object NonExistingAggregate: AggregateReadResult()
}

sealed class ExpectedEventNumber {
    object Any : ExpectedEventNumber()
    object AggregateDoesNotExist : ExpectedEventNumber()
    object AggregateExists : ExpectedEventNumber()
    data class Exact(val eventNumber: Long) : ExpectedEventNumber()
}