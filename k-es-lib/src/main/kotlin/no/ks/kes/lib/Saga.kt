package no.ks.kes.lib

import java.time.Instant
import java.util.*
import kotlin.reflect.KClass


abstract class Saga<STATE : Any>(private val stateClass: KClass<STATE>, val serializationId: String) {

    protected var onEventInit = mutableMapOf<String, EventHandler.Initializer<Event<*>, STATE>>()
    protected val onEventApply = mutableMapOf<String, EventHandler.Applicator<Event<*>, STATE>>()
    protected val onTimeoutApply = mutableMapOf<String, (s: ApplyContext<STATE>) -> ApplyContext<STATE>>()

    @Suppress("UNCHECKED_CAST")
    fun handleEvent(wrapper: EventWrapper<Event<*>>, stateProvider: (correlationId: UUID, stateClass: KClass<*>) -> Any?): SagaRepository.SagaUpsert? {
        val correlationIds = (onEventInit + onEventApply)
                .filter { it.key == AnnotationUtil.getSerializationId(wrapper.event::class) }
                .map { it.value }
                .map { it.correlationId.invoke(wrapper) }
                .distinct()

        val sagaState = when {
            //this saga does not handle this event
            correlationIds.isEmpty() -> return null
            //each handler in a saga must produce the same correlation id
            correlationIds.size > 1 -> error("applying the event ${AnnotationUtil.getSerializationId(wrapper.event::class)} to the event-handlers in ${this::class.simpleName} produced non-identical correlation-ids, please verify the saga configuration")
            //let's see if there's a state for this saga
            else -> stateProvider.invoke(correlationIds.single(), stateClass)
        } as STATE?

        return if (sagaState == null) {
            //non existing saga state, attempting initialization
            onEventInit[wrapper.event::class.serializationId]
                    ?.let {
                        val context = it.handler.invoke(wrapper, InitContext())
                        SagaRepository.SagaUpsert.SagaInsert(
                                correlationId = it.correlationId.invoke(wrapper),
                                serializationId = serializationId,
                                newState = context.newState!!,
                                commands = context.commands
                        )
                    }
        } else {
            //pre-existing state, applying
            onEventApply[wrapper.event::class.serializationId]
                    ?.let {
                        val context = it.handler.invoke(wrapper, ApplyContext(sagaState))
                        if (context.newState == null && context.commands.isEmpty() && context.timeouts.isEmpty())
                            null
                        else
                            SagaRepository.SagaUpsert.SagaUpdate(
                                    correlationId = it.correlationId.invoke(wrapper),
                                    serializationId = serializationId,
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
    ): SagaRepository.SagaUpsert.SagaUpdate? =
            if (timeout.sagaSerializationId != serializationId)
                null
            else
                onTimeoutApply[timeout.timeoutId]
                        ?.invoke(ApplyContext((stateProvider.invoke(timeout.sagaCorrelationId, stateClass)
                                ?: error("A timeout was triggered, but the saga-repository does not contain the saga state: $timeout")) as STATE))
                        ?.let {
                            SagaRepository.SagaUpsert.SagaUpdate(
                                    correlationId = timeout.sagaCorrelationId,
                                    serializationId = timeout.sagaSerializationId,
                                    newState = it.newState,
                                    commands = it.commands,
                                    timeouts = it.timeouts.toSet()
                            )
                        }

    protected inline fun <reified E : Event<*>> init(crossinline correlationId: (E) -> UUID = { it.aggregateId }, crossinline initializer: InitContext<STATE>.(E) -> Unit) =
            initWrapped({ correlationId.invoke(it.event) }, { w: EventWrapper<E> -> initializer.invoke(this, w.event) })

    protected inline fun <reified E : Event<*>> apply(crossinline correlationId: (E) -> UUID = { it.aggregateId }, crossinline handler: ApplyContext<STATE>.(E) -> Unit) =
            applyWrapped({ correlationId.invoke(it.event) }, { w: EventWrapper<E> -> handler.invoke(this, w.event) })

    protected inline fun <reified E : Event<*>> timeout(crossinline correlationId: (E) -> UUID = { it.aggregateId }, crossinline timeoutAt: (E) -> Instant, crossinline handler: ApplyContext<STATE>.() -> Unit) {
        timeoutWrapped<E>({ correlationId.invoke(it.event) }, { timeoutAt.invoke(it.event) }, handler)
    }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : Event<*>> timeoutWrapped(
            crossinline correlationId: (EventWrapper<E>) -> UUID = { it.event.aggregateId },
            crossinline timeoutAt: (EventWrapper<E>) -> Instant,
            crossinline handler: ApplyContext<STATE>.() -> Unit
    ) {
        checkConfig(E::class, HandlerType.APPLY_TIMEOUT)
        onEventApply[E::class.serializationId] = EventHandler.Applicator(
                correlationId = { correlationId.invoke(it as EventWrapper<E>) },
                handler = { e, p -> p.apply { timeouts.add(Timeout(timeoutAt.invoke(e as EventWrapper<E>), AnnotationUtil.getSerializationId(E::class))) } }
        )

        onTimeoutApply[AnnotationUtil.getSerializationId(E::class)] = { context -> handler.invoke(context); context }
    }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : Event<*>> initWrapped(
            crossinline correlationId: (EventWrapper<E>) -> UUID = { it.event.aggregateId },
            noinline handler: InitContext<STATE>.(EventWrapper<E>) -> Unit
    ) {
        checkConfig(E::class, HandlerType.INIT)
        onEventInit[E::class.serializationId] = EventHandler.Initializer(
                correlationId = { correlationId.invoke(it as EventWrapper<E>) },
                handler = { e, context -> handler.invoke(context, e as EventWrapper<E>); context }
        )
    }

    protected fun <E: Event<*>> checkConfig(eventClass: KClass<E>, type: HandlerType) {
        check(!eventClass.deprecated) { "${type.readable} handler in saga ${this::class.simpleName} handles deprecated event with serialization-id ${eventClass.serializationId}, please update the saga configuraton" }
        check(!when(type){
            HandlerType.INIT -> onEventInit
            HandlerType.APPLY_TIMEOUT -> onEventApply
        }.containsKey(eventClass.serializationId)) { "Duplicate ${type.readable} handler for event with serialization-id ${eventClass.serializationId} in saga ${this::class.simpleName}, each event-type can only have one ${type.readable}-handler in a saga configuration" }
    }

    protected enum class HandlerType(val readable: String){INIT("init"), APPLY_TIMEOUT("apply/timeout")}

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : Event<*>> applyWrapped(
            crossinline correlationId: (EventWrapper<E>) -> UUID = { it.event.aggregateId },
            crossinline handler: ApplyContext<STATE>.(EventWrapper<E>) -> Unit
    ) {
        checkConfig(E::class, HandlerType.APPLY_TIMEOUT)
        onEventApply[E::class.serializationId] = EventHandler.Applicator(
                correlationId = { correlationId.invoke(it as EventWrapper<E>) },
                handler = { e, context -> handler.invoke(context, e as EventWrapper<E>); context }
        )
    }

    sealed class EventHandler<E : Event<*>> {
        abstract val correlationId: (EventWrapper<E>) -> UUID

        data class Applicator<E : Event<*>, S : Any>(
                override val correlationId: (EventWrapper<E>) -> UUID,
                val handler: (e: EventWrapper<E>, context: ApplyContext<S>) -> ApplyContext<S>
        ) : EventHandler<E>()

        data class Initializer<E : Event<*>, S : Any>(
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

