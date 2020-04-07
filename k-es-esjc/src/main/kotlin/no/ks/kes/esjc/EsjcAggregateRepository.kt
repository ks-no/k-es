package no.ks.kes.esjc

import com.github.msemys.esjc.EventData
import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.ExpectedVersion
import com.github.msemys.esjc.operation.StreamNotFoundException
import mu.KotlinLogging
import no.ks.kes.lib.*
import java.util.*
import kotlin.streams.asSequence

private const val FIRST_EVENT = 0L
private const val BATCH_SIZE = 100

private val log = KotlinLogging.logger {}

class EsjcAggregateRepository(
        private val eventStore: EventStore,
        private val deserializer: EventSerdes<String>,
        private val streamIdGenerator: (aggregateType: String, aggregateId: UUID) -> String
) : AggregateRepository() {

    override fun append(aggregateType: String, aggregateId: UUID, expectedEventNumber: ExpectedEventNumber, events: List<Event<*>>) {
        val streamId = streamIdGenerator.invoke(aggregateType, aggregateId)
        try {
            eventStore.appendToStream(
                    streamId,
                    resolveExpectedEventNumber(expectedEventNumber),
                    events.map {
                        EventData.newBuilder()
                                .jsonData(deserializer.serialize(it))
                                .type(AnnotationUtil.getSerializationId(it::class))
                                .build()
                    })
                    .get().also {
                        log.info("wrote ${events.size} events to stream ${streamId}, next expected version for this stream is ${it.nextExpectedVersion}")
                    }
        } catch (e: Exception) {
            throw RuntimeException("Error while appending events to stream $streamId", e)
        }
    }

    override fun <A : Aggregate> read(aggregateId: UUID, aggregateType: String, applicator: (state: A?, event: EventWrapper<*>) -> A?): AggregateReadResult =
            try {
                eventStore.streamEventsForward(
                        streamIdGenerator.invoke(aggregateType, aggregateId),
                        FIRST_EVENT,
                        BATCH_SIZE,
                        true
                )
                        .asSequence()
                        .filter { !EsjcEventUtil.isIgnorableEvent(it) }
                        .fold(null as Pair<A, Long>?, { a, e ->
                            applicator.invoke(
                                    a?.first,
                                    EventWrapper(deserializer.deserialize(String(e.event.data), e.event.eventType) as Event<A>, e.event.eventNumber)
                            )
                                    ?.let { it to e.event.eventNumber }
                        })
                        ?.let { AggregateReadResult.ExistingAggregate(it.first, it.second) }
                        ?: error("Aggregate $aggregateId has events, but applying event handlers did not produce an aggregate state!")
            } catch (e: StreamNotFoundException) {
                AggregateReadResult.NonExistingAggregate
            }

    private fun resolveExpectedEventNumber(expectedEventNumber: ExpectedEventNumber): Long =
            when (expectedEventNumber) {
                is ExpectedEventNumber.AggregateDoesNotExist -> ExpectedVersion.NO_STREAM
                is ExpectedEventNumber.AggregateExists -> ExpectedVersion.STREAM_EXISTS
                is ExpectedEventNumber.Any -> ExpectedVersion.ANY
                is ExpectedEventNumber.Exact -> expectedEventNumber.eventNumber
            }
}