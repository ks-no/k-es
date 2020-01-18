package no.ks.kes.esjc

import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.operation.StreamNotFoundException
import no.ks.kes.lib.*
import java.util.*
import kotlin.streams.asSequence

private const val FROM_EVENT_NUMBER = 0L
private const val BATCH_SIZE = 100

class EsjcAggregateReader(
        private val eventStore: EventStore,
        private val deserializer: EventSerdes<String>,
        private val esjcStreamIdGenerator: (aggregateType: String, aggregateId: UUID) -> String
) : AggregateReader {
    override fun <A : Aggregate> read(aggregateId: UUID, aggregate: A): A =
            try {
                eventStore.streamEventsForward(
                        esjcStreamIdGenerator.invoke(aggregate.aggregateType, aggregateId),
                        FROM_EVENT_NUMBER,
                        BATCH_SIZE,
                        true
                )
                        .asSequence()
                        .filter { !EsjcEventUtil.isIgnorableEvent(it) }
                        .fold(aggregate, { a, e ->
                            @Suppress("UNCHECKED_CAST")
                            a.applyEvent(deserializer.deserialize(String(e.event.data), e.event.eventType) as Event<A>, e.event.eventNumber)
                        })
            } catch (e: StreamNotFoundException) {
                aggregate.withCurrentEventNumber(-1)
            }
}