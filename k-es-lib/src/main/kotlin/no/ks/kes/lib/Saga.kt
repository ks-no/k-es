package no.ks.kes.lib

import mu.KotlinLogging
import java.time.Instant
import java.util.*
import kotlin.reflect.KClass

abstract class Saga<STATE : Any>(private val stateClass: KClass<STATE>, val serializationId: String) {

    protected var eventInitializers = mutableListOf<Pair<KClass<EventData<*>>, EventHandler.Initializer<EventData<*>, STATE>>>()
    protected val eventApplicators = mutableListOf<Pair<KClass<EventData<*>>, EventHandler.Applicator<EventData<*>, STATE>>>()
    protected val timeoutApplicators = mutableListOf<Pair<KClass<EventData<*>>, (s: ApplyContext<STATE>) -> ApplyContext<STATE>>>()

    internal fun getConfiguration(serializationIdFunction: (KClass<EventData<*>>) -> String): ValidatedSagaConfiguration<STATE> =
            ValidatedSagaConfiguration(
                    stateClass = stateClass,
                    sagaSerializationId = serializationId,
                    eventInitializers = eventInitializers,
                    eventApplicators = eventApplicators,
                    timeoutApplicators = timeoutApplicators,
                    serializationIdFunction = serializationIdFunction
            )

    protected inline fun <reified E : EventData<*>> init(crossinline correlationId: (E, UUID) -> UUID = { _: EventData<*>, aggregateId: UUID -> aggregateId }, crossinline initializer: InitContext<STATE>.(E, UUID) -> Unit) =
            initWrapped({ correlationId.invoke(it.event.eventData, it.event.aggregateId) }, { w: EventWrapper<E> -> initializer.invoke(this, w.event.eventData, w.event.aggregateId) })

    protected inline fun <reified E : EventData<*>> apply(crossinline correlationId: (E, UUID) -> UUID = { _: EventData<*>, aggregateId: UUID -> aggregateId }, crossinline handler: ApplyContext<STATE>.(E, UUID) -> Unit) =
            applyWrapped({ correlationId.invoke(it.event.eventData, it.event.aggregateId) }, { w: EventWrapper<E> -> handler.invoke(this, w.event.eventData, w.event.aggregateId) })

    protected inline fun <reified E : EventData<*>> timeout(crossinline correlationId: (E, UUID) -> UUID = { _: EventData<*>, aggregateId: UUID -> aggregateId }, crossinline timeoutAt: (E) -> Instant, crossinline handler: ApplyContext<STATE>.() -> Unit) {
        timeoutWrapped<E>({ correlationId.invoke(it.event.eventData, it.event.aggregateId) }, { timeoutAt.invoke(it.event.eventData) }, handler)
    }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : EventData<*>> timeoutWrapped(
            crossinline correlationId: (EventWrapper<E>) -> UUID = { it.event.aggregateId },
            crossinline timeoutAt: (EventWrapper<E>) -> Instant,
            crossinline handler: ApplyContext<STATE>.() -> Unit
    ) {
        eventApplicators.add(E::class as KClass<EventData<*>> to EventHandler.Applicator(
                correlationId = { correlationId.invoke(it as EventWrapper<E>) },
                handler = { e, p -> p.apply { timeouts.add(Timeout(timeoutAt.invoke(e as EventWrapper<E>), e.serializationId)) } }
        ))

        timeoutApplicators.add(E::class as KClass<EventData<*>> to { context -> handler.invoke(context); context })
    }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : EventData<*>> initWrapped(
            crossinline correlationId: (EventWrapper<E>) -> UUID = { it.event.aggregateId },
            noinline handler: InitContext<STATE>.(EventWrapper<E>) -> Unit
    ) {
        eventInitializers.add(E::class as KClass<EventData<*>> to EventHandler.Initializer(
                correlationId = { correlationId.invoke(it as EventWrapper<E>) },
                handler = { e, context -> handler.invoke(context, e as EventWrapper<E>); context }
        ))
    }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : EventData<*>> applyWrapped(
            crossinline correlationId: (EventWrapper<E>) -> UUID = { it.event.aggregateId },
            crossinline handler: ApplyContext<STATE>.(EventWrapper<E>) -> Unit
    ) {
        eventApplicators.add(E::class as KClass<EventData<*>> to EventHandler.Applicator(
                correlationId = { correlationId.invoke(it as EventWrapper<E>) },
                handler = { e, context -> handler.invoke(context, e as EventWrapper<E>); context }
        ))
    }

    class ValidatedSagaConfiguration<STATE : Any>(
        private val stateClass: KClass<STATE>,
        val sagaSerializationId: String,
        serializationIdFunction: (KClass<EventData<*>>) -> String,
        eventInitializers: List<Pair<KClass<EventData<*>>, EventHandler.Initializer<EventData<*>, STATE>>>,
        eventApplicators: List<Pair<KClass<EventData<*>>, EventHandler.Applicator<EventData<*>, STATE>>>,
        timeoutApplicators: List<Pair<KClass<EventData<*>>, (s: ApplyContext<STATE>) -> ApplyContext<STATE>>>
    ) {
        private val eventInitializers: Map<String, EventHandler.Initializer<EventData<*>, STATE>>
        private val eventApplicators: Map<String, EventHandler.Applicator<EventData<*>, STATE>>
        private val timeoutApplicators: Map<String, (s: ApplyContext<STATE>) -> ApplyContext<STATE>> = timeoutApplicators.map { serializationIdFunction.invoke(it.first) to it.second }.toMap()

        init {
            val deprecatedEvents = (eventApplicators.map { it.first } + eventInitializers.map { it.first }).filter { it.deprecated }.map { it::class.simpleName!! }
            check(deprecatedEvents.isEmpty()) { "Saga $sagaSerializationId handles deprecated event(s) ${deprecatedEvents}, please update the saga configuraton" }

            val duplicateEventApplicators = eventApplicators.map { serializationIdFunction.invoke(it.first) }.groupBy { it }.filter { it.value.size > 1 }.map { it.key }
            check(duplicateEventApplicators.isEmpty()) { "There are multiple \"apply/timeout\" configurations for event-type(s) $duplicateEventApplicators in the configuration of $sagaSerializationId, only a single \"apply/timeout\" handler is allowed for each event type" }
            this.eventApplicators = eventApplicators.map { serializationIdFunction.invoke(it.first) to it.second }.toMap()

            val duplicateEventInitializers = eventInitializers.map { serializationIdFunction.invoke(it.first) }.groupBy { it }.filter { it.value.size > 1 }.map { it.key }.distinct()
            check(duplicateEventInitializers.isEmpty()) { "There are multiple \"init\" configurations for event-type(s) $duplicateEventInitializers in the configuration of $sagaSerializationId, only a single \"init\" handler is allowed for each event type" }
            this.eventInitializers = eventInitializers.map { serializationIdFunction.invoke(it.first) to it.second }.toMap()
        }

        @Suppress("UNCHECKED_CAST")
        fun handleEvent(wrapper: EventWrapper<EventData<*>>, stateProvider: (correlationId: UUID, stateClass: KClass<*>) -> Any?): SagaRepository.Operation? {
            val correlationIds = (eventInitializers + eventApplicators)
                    .filter { it.key == wrapper.serializationId }
                    .map { it.value }
                    .map { it.correlationId.invoke(wrapper) }
                    .distinct()

            val sagaState = when {
                //this saga does not handle this event
                correlationIds.isEmpty() -> return null
                //each handler in a saga must produce the same correlation id
                correlationIds.size > 1 -> error("applying the event ${wrapper.serializationId} to the event-handlers in $sagaSerializationId produced non-identical correlation-ids, please verify the saga configuration")
                //let's see if there's a state for this saga
                else -> stateProvider.invoke(correlationIds.single(), stateClass)
            } as STATE?

            return if (sagaState == null) {
                //non existing saga state, attempting initialization
                eventInitializers[wrapper.serializationId]
                        ?.let {
                            val context = it.handler.invoke(wrapper, InitContext())
                            SagaRepository.Operation.Insert(
                                    correlationId = it.correlationId.invoke(wrapper),
                                    serializationId = sagaSerializationId,
                                    newState = context.newState!!,
                                    commands = context.commands
                            )
                        }
            } else {
                //pre-existing state, applying
                eventApplicators[wrapper.serializationId]
                        ?.let {
                            val context = it.handler.invoke(wrapper, ApplyContext(sagaState))
                            if (context.newState == null && context.commands.isEmpty() && context.timeouts.isEmpty())
                                null
                            else
                                SagaRepository.Operation.SagaUpdate(
                                        correlationId = it.correlationId.invoke(wrapper),
                                        serializationId = sagaSerializationId,
                                        newState = context.newState,
                                        commands = context.commands,
                                        timeouts = context.timeouts.toSet()
                                )
                        }
            }
        }

        @Suppress("UNCHECKED_CAST")
        internal fun handleTimeout(
                timeout: SagaRepository.Timeout,
                stateProvider: (correlationId: UUID, stateClass: KClass<*>) -> Any?
        ): SagaRepository.Operation.SagaUpdate? =
                if (timeout.sagaSerializationId != sagaSerializationId)
                    null
                else
                    timeoutApplicators[timeout.timeoutId]
                            ?.invoke(ApplyContext((stateProvider.invoke(timeout.sagaCorrelationId, stateClass)
                                    ?: error("A timeout was triggered, but the saga-repository does not contain the saga state: $timeout")) as STATE))
                            ?.let {
                                SagaRepository.Operation.SagaUpdate(
                                        correlationId = timeout.sagaCorrelationId,
                                        serializationId = timeout.sagaSerializationId,
                                        newState = it.newState,
                                        commands = it.commands,
                                        timeouts = it.timeouts.toSet()
                                )
                            }
    }

    sealed class EventHandler<E : EventData<*>> {
        abstract val correlationId: (EventWrapper<E>) -> UUID

        data class Applicator<E : EventData<*>, S : Any>(
                override val correlationId: (EventWrapper<E>) -> UUID,
                val handler: (e: EventWrapper<E>, context: ApplyContext<S>) -> ApplyContext<S>
        ) : EventHandler<E>()

        data class Initializer<E : EventData<*>, S : Any>(
                override val correlationId: (EventWrapper<E>) -> UUID,
                val handler: (e: EventWrapper<E>, InitContext<S>) -> InitContext<S>
        ) : EventHandler<E>()
    }

    data class Timeout(val triggerAt: Instant, val timeoutId: String)

    class ApplyContext<S : Any>(val state: S) {
        val commands = mutableListOf<Cmd<*>>()
        var newState: S? = null
        val timeouts = mutableListOf<Timeout>()

        fun dispatch(cmd: Cmd<*>) {
            commands.add(cmd)
        }

        fun setState(state: S) {
            newState = state
        }
    }

    class InitContext<S : Any> {
        var newState: S? = null
        val commands = mutableListOf<Cmd<*>>()

        fun dispatch(cmd: Cmd<*>) {
            commands.add(cmd)
        }

        fun setState(state: S) {
            newState = state
        }
    }
}

