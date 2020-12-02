package no.ks.kes.test

import no.ks.kes.lib.*
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.reflect.KClass

private val LOG = mu.KotlinLogging.logger {  }

class KesTestSetup(val eventSerdes: EventSerdes, val cmdSerdes: CmdSerdes): AutoCloseable {
    private val eventStream = TestEventStream()

    val subscriberFactory: EventSubscriberFactory<TestEventSubscription>  by lazy {
        TestEventSubscriberFactory(eventSerdes, eventStream)
    }

    val aggregateRepository: AggregateRepository by lazy {
        TestAggregateRepository(eventSerdes, eventStream)
    }
    override fun close() {
        eventStream.close()
    }
}

data class AggregateKey(val type: String, val aggregateId: UUID)
class TestEventStream: AutoCloseable {
    private val stream: MutableMap<AggregateKey, List<Event<*>>> = mutableMapOf()
    private val listeners = mutableSetOf<EventListener>()

    fun add(aggregateKey: AggregateKey, events: List<Event<*>>) {
        stream.getOrDefault(aggregateKey, emptyList()).apply {
            stream[aggregateKey] = this.plus(events)
        }.also {
            events.forEach { event ->
                LOG.debug { "Sender event" }
                listeners.forEach { listener ->
                    listener.eventAdded(event)
                }
            }
        }
    }

    fun get(aggregateKey: AggregateKey): List<Event<*>>? = stream[aggregateKey]

    fun eventCount(): Long = stream.map { it.value.size }.sum().toLong()

    fun addListener(eventListener: EventListener) {
        listeners.add(eventListener)
    }

    fun removeListener(eventListener: EventListener) {
        listeners.remove(eventListener)
    }

    override fun close() {
        listeners.clear()
    }
}

interface EventListener {
    fun eventAdded(event: Event<*>)
}

class TestEventSubscription(private val factory: TestEventSubscriberFactory,
                            private val onEvent: (EventWrapper<Event<*>>) -> Unit,
                            private val closeHandler: (TestEventSubscription) -> Unit
) : EventSubscription, EventListener, AutoCloseable {
    private val lastProcessedEvent = AtomicLong(-1)
    override fun lastProcessedEvent(): Long = lastProcessedEvent.get()
    override fun eventAdded(event: Event<*>) {
        EventUpgrader.upgrade(event).run {
            onEvent.invoke(EventWrapper(event = this,
                    eventNumber = lastProcessedEvent.getAndIncrement(),
                    serializationId = factory.getSerializationId(this::class as KClass<Event<*>>)
            ))
        }
    }

    override fun close() {
        closeHandler.invoke(this)
    }
}

class TestEventSubscriberFactory(private val serdes: EventSerdes, private val testEventStream: TestEventStream) : EventSubscriberFactory<TestEventSubscription> {
    override fun getSerializationId(eventClass: KClass<Event<*>>): String = serdes.getSerializationId(eventClass)

    override fun createSubscriber(subscriber: String, fromEvent: Long, onEvent: (EventWrapper<Event<*>>) -> Unit, onClose: (Exception) -> Unit, onLive: () -> Unit): TestEventSubscription {
        return TestEventSubscription(factory = this, onEvent = onEvent){
            testEventStream.removeListener(it)
        }.also { testEventStream.addListener(it) }
    }

}

internal class TestAggregateRepository(private val eventSerdes: EventSerdes, private val testEventStream: TestEventStream) : AggregateRepository() {

    override fun getSerializationId(eventClass: KClass<Event<*>>) = eventSerdes.getSerializationId(eventClass)

    override fun append(aggregateType: String, aggregateId: UUID, expectedEventNumber: ExpectedEventNumber, events: List<Event<*>>) {
        AggregateKey(aggregateType, aggregateId).run {
            addEvent(this, events)
        }
    }

    override fun <A : Aggregate> read(aggregateId: UUID, aggregateType: String, applicator: (state: A?, event: EventWrapper<*>) -> A?): AggregateReadResult =
            testEventStream.get(AggregateKey(aggregateType, aggregateId))?.fold(null as A? to null as Long?, { a, e ->
                applicator.invoke(a.first, EventWrapper(e, getEventIndex(), eventSerdes.getSerializationId(e::class))) to getEventIndex()
            })?.let {
                when {
                    //when the aggregate stream has events, but applying these did not lead to a initialized state
                    it.first == null && it.second != null -> AggregateReadResult.UninitializedAggregate(it.second!!)

                    //when the aggregate stream has events, and applying these has lead to a initialized state
                    it.first != null && it.second != null -> AggregateReadResult.InitializedAggregate(it.first!!, it.second!!)

                    //when the aggregate stream has no events
                    else -> error("Nothing to read")
                }
            } ?: AggregateReadResult.NonExistingAggregate


    private fun getEventIndex() = testEventStream.eventCount()

    private fun addEvent(aggregateKey: AggregateKey, events: List<Event<*>>) {
        testEventStream.add(aggregateKey, events)
    }

}