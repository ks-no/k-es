package no.ks.kes.test

import no.ks.kes.lib.*
import no.ks.kes.serdes.jackson.JacksonCmdSerdes
import no.ks.kes.serdes.jackson.JacksonEventSerdes
import no.ks.kes.serdes.jackson.JacksonSagaStateSerdes
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import kotlin.reflect.KClass

private val LOG = mu.KotlinLogging.logger { }

suspend fun withKes(eventSerdes: EventSerdes<EventMetadata>, cmdSerdes: CmdSerdes, context: suspend (KesTestSetup) -> Unit) {
    KesTestSetup(eventSerdes, cmdSerdes).use {
        context.invoke(it)
    }
}

suspend fun withKes(cmds: Set<KClass<out Cmd<*>>>, events: Set<KClass<out Event<*>>>, context: suspend (KesTestSetup) -> Unit) {
    withKes(eventSerdes = JacksonEventSerdes(events = events), cmdSerdes = JacksonCmdSerdes(cmds), context)
}

class KesTestSetup(val eventSerdes: EventSerdes<EventMetadata>, val cmdSerdes: CmdSerdes) : AutoCloseable {
    val eventStream = TestEventStream()

    val subscriberFactory: EventSubscriberFactory<TestEventSubscription> by lazy {
        TestEventSubscriberFactory(eventSerdes, eventStream)
    }

    val aggregateRepository: AggregateRepository by lazy {
        TestAggregateRepository(eventSerdes, eventStream)
    }

    fun createCommandQueue(cmdHandlers: Set<CmdHandler<*>> = emptySet()) = TestCommandQueue(cmdHandlers, cmdSerdes)
    fun createSagaRepository(commandQueue: TestCommandQueue, sagaStateSerdes: SagaStateSerdes = JacksonSagaStateSerdes()) =
            TestSagaRepository(sagaStateSerdes = sagaStateSerdes) { aggregateId, command ->
        commandQueue.addToQueue(aggregateId, command)
    }

    val projectionRepository = TestProjectionRepository()

    override fun close() {
        eventStream.close()
    }
}

data class AggregateKey(val type: String, val aggregateId: UUID)
class TestEventStream : AutoCloseable {
    private val stream: MutableMap<AggregateKey, List<WriteEventWrapper<Event<*>>>> = mutableMapOf()
    private val listeners = mutableSetOf<EventListener>()

    fun add(aggregateKey: AggregateKey, events: List<WriteEventWrapper<Event<*>>>) {
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

    fun get(aggregateKey: AggregateKey): List<WriteEventWrapper<Event<*>>>? = stream[aggregateKey]

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
    fun eventAdded(event: WriteEventWrapper<Event<*>>)
}

class TestEventSubscription(private val factory: TestEventSubscriberFactory,
                            private val onEvent: (EventWrapper<Event<*>>) -> Unit,
                            private val closeHandler: (TestEventSubscription) -> Unit
) : EventSubscription, EventListener, AutoCloseable {
    private val lastProcessedEvent = AtomicLong(-1)
    override fun lastProcessedEvent(): Long = lastProcessedEvent.get()
    override fun eventAdded(event: WriteEventWrapper<Event<*>>) {
        EventUpgrader.upgrade(event.event).run {
            onEvent.invoke(EventWrapper(
                    aggregateId = UUID.randomUUID(),
                    event = this,
                    eventNumber = lastProcessedEvent.getAndIncrement(),
                    serializationId = factory.getSerializationId(this::class as KClass<Event<*>>)
            ))
        }
    }

    override fun close() {
        closeHandler.invoke(this)
    }
}

class TestEventSubscriberFactory(private val serdes: EventSerdes<EventMetadata>, private val testEventStream: TestEventStream) : EventSubscriberFactory<TestEventSubscription> {
    override fun getSerializationId(eventClass: KClass<Event<*>>): String = serdes.getSerializationId(eventClass)

    override fun createSubscriber(subscriber: String, fromEvent: Long, onEvent: (EventWrapper<Event<*>>) -> Unit, onClose: (Exception) -> Unit, onLive: () -> Unit): TestEventSubscription {
        return TestEventSubscription(factory = this, onEvent = onEvent) {
            testEventStream.removeListener(it)
        }.also { testEventStream.addListener(it) }
    }

}

internal class TestAggregateRepository(private val eventSerdes: EventSerdes<EventMetadata>, private val testEventStream: TestEventStream) : AggregateRepository() {

    override fun getSerializationId(eventClass: KClass<Event<*>>) = eventSerdes.getSerializationId(eventClass)

    override fun append(aggregateType: String, aggregateId: UUID, expectedEventNumber: ExpectedEventNumber, events: List<WriteEventWrapper<Event<*>>>) {
        AggregateKey(aggregateType, aggregateId).run {
            addEvent(this, events)
        }
    }

    override fun <A : Aggregate> read(aggregateId: UUID, aggregateType: String, applicator: (state: A?, event: EventWrapper<*>) -> A?): AggregateReadResult =
            testEventStream.get(AggregateKey(aggregateType, aggregateId))?.fold(null as A? to null as Long?, { a, e ->
                applicator.invoke(a.first, EventWrapper(aggregateId,e.event,null, getEventIndex(), eventSerdes.getSerializationId(e.event::class))) to getEventIndex()
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

    private fun addEvent(aggregateKey: AggregateKey, events: List<WriteEventWrapper<Event<*>>>) {
        testEventStream.add(aggregateKey, events)
    }

}

class TestHwmTrackerRepository : HwmTrackerRepository {

    private val subscriptionIndex: MutableMap<String, AtomicLong> = mutableMapOf()
    override fun current(subscriber: String): Long? = subscriptionIndex[subscriber]?.get()

    override fun getOrInit(subscriber: String): Long = subscriptionIndex.getOrDefault(subscriber, AtomicLong(-1))
            .apply {
                subscriptionIndex[subscriber] = this
            }.let {
                it.get()
            }

    override fun update(subscriber: String, hwm: Long) {
        subscriptionIndex[subscriber]?.apply {
            this.set(hwm)
        } ?: error("""Can not update hwm "$subscriber" that has not been initialized""")
    }
}

class TestProjectionRepository : ProjectionRepository {
    override val hwmTracker: HwmTrackerRepository = TestHwmTrackerRepository()

    override fun transactionally(runnable: () -> Unit) {
        runnable.invoke()
    }
}


internal data class CmdStatus(val aggregateId: UUID, val serializationId: String, val errorId: UUID? = null, val retries: Int = 0, val nextExecution: Instant, val deserializedCommd: String)
class TestCommandQueue(cmdHandlers: Set<CmdHandler<*>>, private val cmdSerdes: CmdSerdes) : CommandQueue(cmdHandlers) {

    private val cmdIdSequence = AtomicLong(0L)
    private val commandQueue = mutableMapOf<Long, CmdStatus>()
    private val commandsAwaiting
        get() = commandQueue.filterValues { Instant.now().isAfter(it.nextExecution) }.toSortedMap(compareBy { it })

    override fun delete(cmdId: Long) {
        commandQueue.minusAssign(cmdId)
    }

    override fun incrementAndSetError(cmdId: Long, errorId: UUID) {
        commandQueue[cmdId]?.apply {
            commandQueue[cmdId] = copy(errorId = errorId)
        }
    }

    override fun incrementAndSetNextExecution(cmdId: Long, nextExecution: Instant) {
        commandQueue[cmdId]?.apply {
            commandQueue[cmdId] = copy(nextExecution = nextExecution, retries = retries + 1)
        }
    }

    override fun nextCmd(): CmdWrapper<Cmd<*>>? = commandsAwaiting.toList().firstOrNull()?.let {
        CmdWrapper(id = it.first, retries = it.second.retries, cmd = cmdSerdes.deserialize(it.second.deserializedCommd.toByteArray(), it.second.serializationId))
    }.also { LOG.debug { "nextCmd is: $it" } }


    override fun transactionally(runnable: () -> Unit) {
        runnable.invoke()
    }

    fun addToQueue(aggregateId: UUID, cmd: Cmd<*>) {
        commandQueue.plusAssign(cmdIdSequence.incrementAndGet() to CmdStatus(
                aggregateId = aggregateId,
                serializationId = cmdSerdes.getSerializationId(cmd::class as KClass<Cmd<*>>),
                nextExecution = Instant.now(),
                deserializedCommd = cmdSerdes.serialize(cmd).toString(StandardCharsets.UTF_8)
        ))
    }
}

class TestSagaRepository(private val sagaStateSerdes: SagaStateSerdes, private val addCommandToQueue: (UUID, Cmd<*>) -> Unit) : SagaRepository {

    private val scheduledExecutor = Executors.newScheduledThreadPool(5)
    private val sagaStates = mutableMapOf<SagaKey, ByteArray>()

    private data class SagaKey(val correlationId: UUID, val serializationId: String)

    private val timeouts = mutableMapOf<TimeoutKey, TimeoutEntry>()

    private data class TimeoutKey(val serializationId: String, val correlationId: UUID, val timeoutId: String)
    private data class TimeoutEntry(val timeout: Instant, val error: Int = 0)

    override fun <T : Any> getSagaState(correlationId: UUID, serializationId: String, sagaStateClass: KClass<T>): T? =
            sagaStates[SagaKey(correlationId, serializationId)]?.let {
                sagaStateSerdes.deserialize(it, sagaStateClass)
            }


    override fun update(states: Set<SagaRepository.Operation>) {
        LOG.info { "Updating sagas: $states" }
        states.filterIsInstance<SagaRepository.Operation.Insert>()
                .forEach {
                    sagaStates.plusAssign(SagaKey(it.correlationId, it.serializationId) to sagaStateSerdes.serialize(it.newState))
                }

        states.filterIsInstance<SagaRepository.Operation.SagaUpdate>().forEach {
            it.timeouts.forEach { timeout ->
                timeouts.plusAssign(TimeoutKey(correlationId = it.correlationId, serializationId = it.serializationId, timeoutId = timeout.timeoutId) to TimeoutEntry(timeout = Instant.now()))

            }
        }

        states.filterIsInstance<SagaRepository.Operation.SagaUpdate>().forEach { sagaUpdate ->
            SagaKey(correlationId = sagaUpdate.correlationId, serializationId = sagaUpdate.serializationId).let { sagaKey ->
                if (sagaStates.containsKey(sagaKey)) {
                    sagaUpdate?.newState?.apply {
                        sagaStates[sagaKey] = sagaStateSerdes.serialize(this)
                    }
                }
            }
        }
        states.flatMap { it.commands }.forEach {
            addCommandToQueue.invoke(it.aggregateId, it)
        }
    }

    override fun getReadyTimeouts(): SagaRepository.Timeout? =
            timeouts.filter { entry -> entry.value.error == 0 && entry.value.timeout.isBefore(Instant.now()) }.toList().firstOrNull()?.let {
                SagaRepository.Timeout(it.first.correlationId, it.first.serializationId, it.first.timeoutId)
            }

    override fun deleteTimeout(timeout: SagaRepository.Timeout) {
        timeouts.minusAssign(TimeoutKey(serializationId = timeout.sagaSerializationId, correlationId = timeout.sagaCorrelationId, timeoutId = timeout.timeoutId))
    }

    override fun transactionally(runnable: () -> Unit) {
        runnable.invoke()
    }

    override val hwmTracker: HwmTrackerRepository = TestHwmTrackerRepository()
}

