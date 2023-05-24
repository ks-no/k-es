package no.ks.kes.grpc

import com.eventstore.dbclient.*
import com.eventstore.dbclient.ExpectedRevision.expectedRevision
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.reactivex.rxjava3.core.Flowable
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
    private val metadataSerdes: EventMetadataSerdes<out Metadata>? = null,
    private val allowRetryOnWrite: Boolean = false,
) : AggregateRepository() {

    override fun append(aggregateType: String, aggregateId: UUID, expectedEventNumber: ExpectedEventNumber, eventWrappers: List<Event<*>>) {
        try {
            appendEventsToStream(aggregateType, aggregateId, eventWrappers, expectedEventNumber)
        } catch (e: WriteAbortedException) {
            if (allowRetryOnWrite) {
                log.info { "Retrying write events to stream for aggregateType: $aggregateType, aggregateId: '$aggregateId'" }
                appendEventsToStream(aggregateType, aggregateId, eventWrappers, expectedEventNumber)
            } else {
                log.error(e) { "Got aborted status when we were writing data to stream, with no retry. aggregateType: $aggregateType, aggregateId: '$aggregateId'" }
                throw e
            }
        }
    }

    private fun appendEventsToStream(
        aggregateType: String,
        aggregateId: UUID,
        eventWrappers: List<Event<*>>,
        expectedEventNumber: ExpectedEventNumber
    ) {
        val streamId = streamIdGenerator.invoke(aggregateType, aggregateId)
        val events = eventWrappers.map { toEventData(it, serdes) }
        try {
            eventStoreDBClient
                .appendToStream(
                    streamId,
                    AppendToStreamOptions.get().expectedRevision(resolveExpectedRevision(expectedEventNumber)),
                    events.iterator()
                )
                .get()
                .also {
                    log.info("wrote ${eventWrappers.size} events to stream ${streamId}, next expected version for this stream is ${it.nextExpectedRevision}")
                }

        } catch (e: ExecutionException) {
            val cause = e.cause
            if (cause is WrongExpectedVersionException) {
                throw RuntimeException(
                    "Actual version did not match expected! streamName: ${cause.streamName}, nextExpectedRevision: ${cause.nextExpectedRevision}, actualVersion: ${cause.actualVersion}",
                    e
                )
            } else if (cause is StatusRuntimeException && cause.status == Status.ABORTED) {
                throw WriteAbortedException("Got aborted status when we were writing data to stream",cause)
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
            if (metadataSerdes != null && event.metadata != null) {
                metadataAsBytes(metadataSerdes.serialize(event.metadata!!))
            }
        }.build()

    private fun resolveExpectedRevision(expectedEventNumber: ExpectedEventNumber): ExpectedRevision =
        when (expectedEventNumber) {
            is ExpectedEventNumber.AggregateDoesNotExist -> ExpectedRevision.noStream()
            is ExpectedEventNumber.AggregateExists -> ExpectedRevision.streamExists()
            is ExpectedEventNumber.Any -> ExpectedRevision.any()
            is ExpectedEventNumber.Exact -> expectedRevision(expectedEventNumber.eventNumber)
        }

    override fun getSerializationId(eventDataClass: KClass<EventData<*>>): String =
        serdes.getSerializationId(eventDataClass)

    override fun <A : Aggregate> read(aggregateId: UUID, aggregateType: String, applicator: (state: A?, event: EventWrapper<*>) -> A?): AggregateReadResult {
        val streamId = streamIdGenerator.invoke(aggregateType, aggregateId)

        return try {
            Flowable
                .fromPublisher(
                    eventStoreDBClient.readStreamReactive(
                        streamId,
                        ReadStreamOptions.get()
                            .forwards()
                            .fromStart()
                            .notResolveLinkTos()
                    )
                )
                .reduce(AggregateContext(aggregateId, streamId, applicator)) { aggregate, message ->
                    handleMessage(aggregate, message)
                }
                .blockingGet()
                .let { toAggregateReadResult(it.state, it.lastStreamPosition, streamId) }
        } catch (e: Exception) {
            when (e) {
                is StreamNotFoundException -> AggregateReadResult.NonExistingAggregate
                else -> throw e
            }
        }
    }

    private fun <A : Aggregate> handleMessage(context: AggregateContext<A>, message: ReadMessage): AggregateContext<A> {
        log.trace { "handleMessage: ${message.toLogString()}" }
        return if (message.hasEvent()) {
            context.apply {
                state = handleEvent(context, message.event)
                lastStreamPosition = message.event?.event?.revision ?: context.lastStreamPosition
            }
        } else {
            log.debug { "Message does not have event (context: $context)" }
            context
        }
    }

    private fun <A : Aggregate> handleEvent(context: AggregateContext<A>, event: ResolvedEvent): A? =
        if (!event.isIgnorable()) {
            handleEvent(context, event.event)
        } else {
            context.state
        }

    private fun <A : Aggregate> handleEvent(context: AggregateContext<A>, event: RecordedEvent): A? {
        val metadata = getMetadata(event)
        val eventData = EventUpgrader.upgrade(serdes.deserialize(event.eventData, event.eventType))
        return context.applicator.invoke(
            context.state,
            EventWrapper(
                Event(
                    aggregateId = context.aggregateId,
                    eventData = eventData,
                    metadata = metadata
                ),
                eventNumber = event.revision,
                serializationId = serdes.getSerializationId(eventData::class)
            )
        )
    }

    private fun getMetadata(event: RecordedEvent) =
        if (event.userMetadata.isNotEmpty() && metadataSerdes != null)
            metadataSerdes.deserialize(event.userMetadata)
        else
            null

    private fun ReadMessage.toLogString() = "ReadMessage(" +
            "firstStreamPosition=${if (hasFirstStreamPosition()) firstStreamPosition else null}, " +
            "lastStreamPosition=${if (hasLastStreamPosition()) lastStreamPosition else null}, " +
            "lastAllPosition=${if (hasLastAllPosition()) lastStreamPosition else null}, " +
            "event=${if (hasEvent()) event else null}" +
            ")"

    private fun <A : Aggregate> toAggregateReadResult(state: A?, lastStreamPosition: Long?, streamId: String): AggregateReadResult {
        return if (lastStreamPosition != null) {
            toAggregateReadResult(state, lastStreamPosition)
        } else {
            // When the aggregate stream has no events
            throw RuntimeException("Error reading ${streamId}, the stream exists but does not contain any events")
        }
    }

    private fun <A : Aggregate> toAggregateReadResult(state: A?, lastStreamPosition: Long): AggregateReadResult {
        return if (state == null) {
            // When the aggregate stream has events, but applying these did not lead to a initialized state
            AggregateReadResult.UninitializedAggregate(lastStreamPosition)
        } else {
            // When the aggregate stream has events, and applying these has lead to a initialized state
            AggregateReadResult.InitializedAggregate(state, lastStreamPosition)
        }
    }
}

private data class AggregateContext<A : Aggregate>(
    val aggregateId: UUID,
    val streamId: String,
    val applicator: (state: A?, event: EventWrapper<*>) -> A?,
    var state: A? = null,
    var lastStreamPosition: Long? = null,
)

