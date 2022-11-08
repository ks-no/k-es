package no.ks.kes.grpc

import com.eventstore.dbclient.*
import com.eventstore.dbclient.ExpectedRevision.expectedRevision
import mu.KotlinLogging
import no.ks.kes.grpc.GrpcEventUtil.isIgnorable
import no.ks.kes.lib.*
import no.ks.kes.lib.EventData
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
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
                events.iterator()
            )
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
            if (metadataSerdes != null && event.metadata != null) {
                metadataAsBytes(metadataSerdes.serialize(event.metadata!!))
            }
        }.build()

    override fun getSerializationId(eventDataClass: KClass<EventData<*>>): String =
        serdes.getSerializationId(eventDataClass)

    override fun <A : Aggregate> read(aggregateId: UUID, aggregateType: String, applicator: (state: A?, event: EventWrapper<*>) -> A?): AggregateReadResult {
        val streamId = streamIdGenerator.invoke(aggregateType, aggregateId)

        return AggregateSubscriber(serdes, metadataSerdes, aggregateId, streamId, applicator)
            .apply {
                eventStoreDBClient.readStreamReactive(
                    streamId,
                    ReadStreamOptions.get()
                        .forwards()
                        .fromStart()
                        .notResolveLinkTos()
                ).subscribe(this)
            }
            .future
            .get(5, TimeUnit.MINUTES)
    }

    private fun resolveExpectedRevision(expectedEventNumber: ExpectedEventNumber): ExpectedRevision =
        when (expectedEventNumber) {
            is ExpectedEventNumber.AggregateDoesNotExist -> ExpectedRevision.noStream()
            is ExpectedEventNumber.AggregateExists -> ExpectedRevision.streamExists()
            is ExpectedEventNumber.Any -> ExpectedRevision.any()
            is ExpectedEventNumber.Exact -> expectedRevision(expectedEventNumber.eventNumber)
        }

}

private class AggregateSubscriber<A : Aggregate>(
    private val serdes: EventSerdes,
    private val metadataSerdes: EventMetadataSerdes<out Metadata>?,
    private val aggregateId: UUID,
    private val streamId: String,
    private val applicator: (state: A?, event: EventWrapper<*>) -> A?
) : Subscriber<ReadMessage> {

    val future = CompletableFuture<AggregateReadResult>()

    private var subscription: Subscription? = null
    private var state: A? = null
    private var lastStreamPosition: Long? = null

    override fun onSubscribe(subscription: Subscription) {
        this.subscription = subscription
        subscription.request(Long.MAX_VALUE)
    }

    override fun onNext(message: ReadMessage) {
        log.trace { "onNext: ${message.toLogString()}" }
        if (message.hasEvent()) {
            this.lastStreamPosition = message.event?.event?.revision ?: lastStreamPosition
            handleEvent(message.event)
        } else {
            log.debug { "Message does not have event (streamId: $streamId, aggregateId: $aggregateId, lastStreamPosition: $lastStreamPosition)" }
        }
    }

    private fun handleEvent(event: ResolvedEvent) {
        if (!event.isIgnorable()) {
            handleEvent(event.event)
        }
    }

    private fun handleEvent(event: RecordedEvent) {
        val metadata = getMetadata(event)
        val eventData = EventUpgrader.upgrade(serdes.deserialize(event.eventData, event.eventType))
        this.state = applicator.invoke(
            this.state,
            EventWrapper(
                Event(
                    aggregateId = aggregateId,
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
                "firstStreamPosition=${ if (hasFirstStreamPosition()) firstStreamPosition else null }, " +
                "lastStreamPosition=${ if (hasLastStreamPosition()) lastStreamPosition else null }, " +
                "lastAllPosition=${ if (hasLastAllPosition()) lastStreamPosition else null }, " +
                "event=${ if (hasEvent()) event else null }" +
            ")"

    override fun onError(throwable: Throwable) {
        when (throwable) {
            is StreamNotFoundException -> this.future.complete(AggregateReadResult.NonExistingAggregate)
            else -> this.future.completeExceptionally(throwable)
        }
    }

    override fun onComplete() {
        if (this.lastStreamPosition != null) {
            completeHandledEvent()
        } else {
            // When the aggregate stream has no events
            this.future.completeExceptionally(RuntimeException("Error reading $streamId, the stream exists but does not contain any events"))
        }
    }

    private fun completeHandledEvent() {
        if (this.state == null) {
            // When the aggregate stream has events, but applying these did not lead to a initialized state
            this.future.complete(AggregateReadResult.UninitializedAggregate(this.lastStreamPosition!!))
        } else {
            // When the aggregate stream has events, and applying these has lead to a initialized state
            this.future.complete(AggregateReadResult.InitializedAggregate(this.state!!, this.lastStreamPosition!!))
        }
    }

}
