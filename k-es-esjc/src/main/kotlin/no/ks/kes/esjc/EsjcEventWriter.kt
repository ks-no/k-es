package no.ks.kes.esjc

import com.github.msemys.esjc.EventData
import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.ExpectedVersion
import mu.KotlinLogging
import no.ks.kes.lib.*
import java.util.*

private val log = KotlinLogging.logger {}

class EsjcEventWriter(
        private val eventStore: EventStore,
        private val streamIdGenerator: (aggregateType: String, aggregateId: UUID) -> String,
        private val deserializer: EventSerdes<String>
) : EventWriter {
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
            when (expectedEventNumber){
                is ExpectedEventNumber.AggregateDoesNotExist -> ExpectedVersion.NO_STREAM
                is ExpectedEventNumber.AggregateExists -> ExpectedVersion.STREAM_EXISTS
                is ExpectedEventNumber.Any -> ExpectedVersion.ANY
                is ExpectedEventNumber.Exact -> expectedEventNumber.eventNumber
            }

}