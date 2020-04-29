package no.ks.kes.esjc

import com.github.msemys.esjc.EventData
import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.ExpectedVersion
import com.github.msemys.esjc.operation.StreamNotFoundException
import mu.KotlinLogging
import no.ks.kes.lib.*
import java.util.*
import kotlin.reflect.KClass
import kotlin.streams.asSequence

private const val FIRST_EVENT = 0L
private const val BATCH_SIZE = 100

private val log = KotlinLogging.logger {}

class EsjcAggregateRepository(
        private val eventStore: EventStore,
        private val serdes: EventSerdes,
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
                                .jsonData(serdes.serialize(it))
                                .type(serdes.getSerializationId(it::class))
                                .build()
                    })
                    .get().also {
                        log.info("wrote ${events.size} events to stream ${streamId}, next expected version for this stream is ${it.nextExpectedVersion}")
                    }
        } catch (e: Exception) {
            throw RuntimeException("Error while appending events to stream $streamId", e)
        }
    }

    override fun getSerializationId(eventClass: KClass<Event<*>>): String = serdes.getSerializationId(eventClass)

    override fun <A : Aggregate> read(aggregateId: UUID, aggregateType: String, applicator: (state: A?, event: EventWrapper<*>) -> A?): AggregateReadResult =
            try {
                val streamId = streamIdGenerator.invoke(aggregateType, aggregateId)
                eventStore.streamEventsForward(
                        streamId,
                        FIRST_EVENT,
                        BATCH_SIZE,
                        false
                )
                        .asSequence()
                        .fold(null as A? to null as Long?, { a, e ->
                            if (EsjcEventUtil.isIgnorableEvent(e)) {
                                a.first to e.event.eventNumber
                            } else {
                                val deserialized = EventUpgrader.upgrade(serdes.deserialize(e.event.data, e.event.eventType))
                                applicator.invoke(
                                        a.first,
                                        EventWrapper(deserialized as Event<A>, e.event.eventNumber, serdes.getSerializationId(deserialized::class))
                                ) to e.event.eventNumber
                            }
                        })
                        .let {
                            when {
                                //when the aggregate has non-ignorable events, but applying these did not lead to a initialized state
                                it.first == null && it.second != null -> AggregateReadResult.UninitializedAggregate(it.second!!)

                                //when the aggregate has non-ignorable events, and applying these has lead to a initialized state
                                it.first != null && it.second != null -> AggregateReadResult.InitializedAggregate(it.first!!, it.second!!)

                                else -> error("Error reading $streamId, the stream exists but does not contain any events")
                            }
                        }
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