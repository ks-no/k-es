package no.ks.kes.esjc

import com.github.msemys.esjc.EventData
import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.ExpectedVersion
import mu.KotlinLogging
import no.ks.kes.lib.Event
import no.ks.kes.lib.EventSerdes
import no.ks.kes.lib.EventUtil
import no.ks.kes.lib.EventWriter
import java.util.*

private val log = KotlinLogging.logger {}

class EsjcEventWriter(
        private val eventStore: EventStore,
        private val streamIdGenerator: (aggregateType: String, aggregateId: UUID) -> String,
        private val deserializer: EventSerdes
) : EventWriter {
    override fun write(aggregateType: String, aggregateId: UUID, expectedEventNumber: Long, events: List<Event<*>>, useOptimisticLocking: Boolean) {
        val streamId = streamIdGenerator.invoke(aggregateType, aggregateId)
        try {
            eventStore.appendToStream(
                    streamId,
                    if (useOptimisticLocking) expectedEventNumber else ExpectedVersion.ANY,
                    events.map {
                        EventData.newBuilder()
                                .jsonData(deserializer.serialize(it))
                                .type(EventUtil.getEventType(it::class))
                                .build()
                    })
                    .get().also {
                        log.info("wrote ${events.size} events to stream ${streamId}, next expected version for this stream is ${it.nextExpectedVersion}")
                    }
        } catch (e: Exception) {
            throw RuntimeException("Error while appending events to stream $streamId", e)
        }
    }

}