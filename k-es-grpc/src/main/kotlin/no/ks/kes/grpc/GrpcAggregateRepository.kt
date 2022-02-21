package no.ks.kes.grpc

import com.eventstore.dbclient.*
import com.eventstore.dbclient.ExpectedRevision.expectedRevision
import mu.KotlinLogging
import no.ks.kes.grpc.GrpcEventUtil.isIgnorable
import no.ks.kes.lib.*
import no.ks.kes.lib.EventData
import java.util.*
import java.util.concurrent.ExecutionException
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

class GrpcAggregateRepository(
    private val eventStoreDBClient: EventStoreDBClient,
    private val serdes: EventSerdes,
    private val streamIdGenerator: (aggregateType: String, aggregateId: UUID) -> String,
    private val metadataSerdes: EventMetadataSerdes<out Metadata>? = null
) : AggregateRepository() {

    override fun append(aggregateType: String, aggregateId: UUID, expectedEventNumber: ExpectedEventNumber, eventWrappers: List<Event<*>>) {
        val streamId = streamIdGenerator.invoke(aggregateType, aggregateId)
        try {
            val events = eventWrappers.map { toEventData(it, serdes) }
            eventStoreDBClient.appendToStream(
                streamId,
                AppendToStreamOptions.get().expectedRevision(resolveExpectedRevision(expectedEventNumber)),
                events.iterator())
                .get().also {
                    log.info("wrote ${eventWrappers.size} events to stream ${streamId}, next expected version for this stream is ${it.nextExpectedRevision}")
                }
        } catch (e: ExecutionException) {
            val cause = e.cause
            if (cause is WrongExpectedVersionException) {
                throw RuntimeException("Actual version did not match expected! streamName: ${cause.streamName}, nextExpectedRevision: ${cause.nextExpectedRevision}, actualVersion: ${cause.actualVersion}", e)
            } else {
                throw RuntimeException("Error while appending events to stream $streamId", cause)
            }
        }
    }

    private fun toEventData(event: Event<*>, serdes: EventSerdes): com.eventstore.dbclient.EventData =
        if (serdes.isJson()) {
            EventDataBuilder.json(serdes.getSerializationId(event.eventData::class), serdes.serialize(event.eventData))
        } else {
            EventDataBuilder.binary(serdes.getSerializationId(event.eventData::class), serdes.serialize(event.eventData))
        }.apply {
            if(metadataSerdes != null && event.metadata != null) {
                metadataAsBytes(metadataSerdes.serialize(event.metadata!!))
            }
        }.build()

    override fun getSerializationId(eventDataClass: KClass<EventData<*>>): String =
        serdes.getSerializationId(eventDataClass)

    override fun <A : Aggregate> read(aggregateId: UUID, aggregateType: String, applicator: (state: A?, event: EventWrapper<*>) -> A?): AggregateReadResult {
        val streamId = streamIdGenerator.invoke(aggregateType, aggregateId)
        val readOptions = ReadStreamOptions.get()
            .forwards()
            .fromStart()
            .notResolveLinkTos()
        return eventStoreDBClient.readStream(streamId, readOptions)
            .handle { result, throwable ->
                if (throwable == null) {
                    result.events
                        .fold(null as A? to null as Long?) { a, e ->
                            if (e.isIgnorable()) {
                                a.first to e.event.streamRevision.valueUnsigned
                            } else {
                                val eventMeta =
                                    if (e.event.userMetadata.isNotEmpty() && metadataSerdes != null) metadataSerdes.deserialize(
                                        e.event.userMetadata
                                    ) else null
                                val event = serdes.deserialize(e.event.eventData, e.event.eventType)
                                val deserialized = EventUpgrader.upgrade(event)
                                applicator.invoke(
                                    a.first,
                                    EventWrapper(
                                        Event(
                                            aggregateId = aggregateId,
                                            eventData = deserialized,
                                            metadata = eventMeta
                                        ),
                                        eventNumber = e.event.streamRevision.valueUnsigned,
                                        serializationId = serdes.getSerializationId(deserialized::class)
                                    )
                                ) to e.event.streamRevision.valueUnsigned
                            }
                        }
                        .let {
                            when {
                                //when the aggregate stream has events, but applying these did not lead to a initialized state
                                it.first == null && it.second != null -> AggregateReadResult.UninitializedAggregate(it.second!!)

                                //when the aggregate stream has events, and applying these has lead to a initialized state
                                it.first != null && it.second != null -> AggregateReadResult.InitializedAggregate(
                                    it.first!!,
                                    it.second!!
                                )

                                //when the aggregate stream has no events
                                else -> error("Error reading $streamId, the stream exists but does not contain any events")
                            }
                        }
                } else {
                    when (throwable) {
                        is StreamNotFoundException -> AggregateReadResult.NonExistingAggregate
                        else -> throw throwable
                    }
                }
            }.get()
    }

    private fun resolveExpectedRevision(expectedEventNumber: ExpectedEventNumber): ExpectedRevision =
        when (expectedEventNumber) {
            is ExpectedEventNumber.AggregateDoesNotExist -> ExpectedRevision.NO_STREAM
            is ExpectedEventNumber.AggregateExists -> ExpectedRevision.STREAM_EXISTS
            is ExpectedEventNumber.Any -> ExpectedRevision.ANY
            is ExpectedEventNumber.Exact -> expectedRevision(expectedEventNumber.eventNumber)
        }

}