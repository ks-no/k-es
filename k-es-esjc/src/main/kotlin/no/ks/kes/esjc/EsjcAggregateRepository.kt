package no.ks.kes.esjc

import com.github.msemys.esjc.EventData
import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.ExpectedVersion
import com.github.msemys.esjc.operation.StreamNotFoundException
import mu.KotlinLogging
import no.ks.kes.lib.*
import java.util.*
import kotlin.streams.asSequence

private const val FROM_EVENT_NUMBER = 0L
private const val BATCH_SIZE = 100

private val log = KotlinLogging.logger {}

class EsjcAggregateRepository(
        private val eventStore: EventStore,
        private val deserializer: EventSerdes<String>,
        private val streamIdGenerator: (aggregateType: String, aggregateId: UUID) -> String
) : AggregateRepository {
    override fun <A : Aggregate> read(aggregateId: UUID, aggregate: A): A =
            try {
                eventStore.streamEventsForward(
                        streamIdGenerator.invoke(aggregate.aggregateType, aggregateId),
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

    override fun write(aggregateType: String, aggregateId: UUID, expectedEventNumber: ExpectedEventNumber, events: List<Event<*>>) {
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

    private fun resolveExpectedEventNumber(expectedEventNumber: ExpectedEventNumber): Long =
            when (expectedEventNumber) {
                is ExpectedEventNumber.AggregateDoesNotExist -> ExpectedVersion.NO_STREAM
                is ExpectedEventNumber.AggregateExists -> ExpectedVersion.STREAM_EXISTS
                is ExpectedEventNumber.Any -> ExpectedVersion.ANY
                is ExpectedEventNumber.Exact -> expectedEventNumber.eventNumber
            }
}