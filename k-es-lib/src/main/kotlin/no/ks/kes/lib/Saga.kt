package no.ks.kes.lib

import mu.KotlinLogging
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

abstract class Saga<STATE : Any>(private val stateClass: KClass<STATE>) {

    protected var initializers = mutableListOf<Initializer<*, STATE>>()
    protected val onEvents = mutableListOf<OnEvent<*, STATE>>()
    protected val createTimeouts = mutableListOf<OnEvent<*, STATE>>()

    fun getConfiguration(): SagaConfiguration<*> {
        val duplicateEvents = (onEvents.map { it.eventClass } + initializers.map { it.eventClass })
                .groupingBy { it }
                .eachCount()
                .filter { it.value > 1 }

        val duplicateTimeouts = (createTimeouts.map { it.eventClass } + initializers.map { it.eventClass })
                .groupingBy { it }
                .eachCount()
                .filter { it.value > 1 }

        when {
            initializers.isEmpty() -> error("No \"initOn\" defined in saga ${this::class.simpleName}. Please define an initializer")
            initializers.size > 1 -> error("Multiple \"initOn\" in saga ${this::class.simpleName}. Please specify a single initializer")
            onEvents.isEmpty() -> log.warn { "No handlers specified in saga ${this::class.simpleName}. Consider adding some?" }
            duplicateEvents.isNotEmpty() -> error("The following events occur multiple times in the \"initOn\" or \"on\" specification in saga ${this::class.simpleName}. Please remove duplicates")
            duplicateTimeouts.isNotEmpty() -> error("The following events occur multiple times in a \"createTimeout\" specification in saga ${this::class.simpleName}. Please remove duplicates")
        }

        @Suppress("UNCHECKED_CAST")
        return SagaConfiguration(
                this::class,
                stateClass as KClass<Any>,
                initializers.single() as Initializer<Event<*>, Any>,
                (onEvents + initializers).toSet() as Set<OnEvent<Event<*>, Any>>
        )
    }

    protected inline fun <reified E : Event<*>> initOn(crossinline correlationId: (E) -> UUID = {it.aggregateId}, crossinline initializer: InitContext<STATE>.(E) -> Unit) =
            initOnWrapper({ correlationId.invoke(it.event) }, { w: EventWrapper<E> -> initializer.invoke(this, w.event) })

    protected inline fun <reified E : Event<*>> on(crossinline correlationId: (E) -> UUID = {it.aggregateId}, crossinline handler: SagaContext<STATE>.(E) -> Unit) =
            onWrapper({ correlationId.invoke(it.event) }, { w: EventWrapper<E> -> handler.invoke(this, w.event) })

    protected inline fun <reified E : Event<*>> createTimeoutOn(crossinline timeoutAt: (E) -> Instant, crossinline correlationId: (E) -> UUID = {it.aggregateId}, crossinline handler: SagaContext<STATE>.() -> Unit) {
        createTimeoutOnWrapper<E>({timeoutAt.invoke(it.event)}, {correlationId.invoke(it.event)}, handler)
    }

    protected inline fun <reified E : Event<*>> createTimeoutOnWrapper(crossinline timeoutAt: (EventWrapper<E>) -> Instant, crossinline correlationId: (EventWrapper<E>) -> UUID = {it.event.aggregateId}, crossinline handler: SagaContext<STATE>.() -> Unit) {
        createTimeouts.add(OnEvent(E::class, {correlationId.invoke(it)}, { e, p -> handler.invoke(p); p.apply{timeouts.add(Timeout(timeoutAt.invoke(e), AnnotationUtil.getSerializationId(E::class)))}}))
    }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : Event<*>> initOnWrapper(crossinline correlationId: (EventWrapper<E>) -> UUID = {it.event.aggregateId}, noinline handler: InitContext<STATE>.(EventWrapper<E>) -> Unit) {
        initializers.add(Initializer(E::class as KClass<Event<*>>, { correlationId.invoke(it as EventWrapper<E>) }, { e, p -> handler.invoke(p, e as EventWrapper<E>); p}))
    }

    protected inline fun <reified E : Event<*>> onWrapper(crossinline correlationId: (EventWrapper<E>) -> UUID = {it.event.aggregateId}, crossinline handler: SagaContext<STATE>.(EventWrapper<E>) -> Unit) {
        onEvents.add(OnEvent(E::class, { correlationId.invoke(it) }, { e, p -> handler.invoke(p, e); p}))
    }

    data class OnEvent<E : Event<*>, S : Any>(val eventClass: KClass<E>, val correlationId: (EventWrapper<E>) -> UUID, val handler: (e: EventWrapper<E>, s: SagaContext<S>) -> SagaContext<S>)
    data class CreateTimeoutOn<E : Event<*>, S : Any>(val eventClass: KClass<E>, val correlationId: (EventWrapper<E>) -> UUID, val handler: (SagaContext<S>) -> Pair<S?, Timeout>)
    data class Timeout(val triggerAt: Instant, val timeoutId: String)
    data class Initializer<E : Event<*>, S: Any>(val eventClass: KClass<E>, val correlationId: (EventWrapper<E>) -> UUID, val handler: (e: EventWrapper<E>, InitContext<S>) -> InitContext<S>)

    data class SagaConfiguration<SAGA : Saga<*>>(val sagaClass: KClass<SAGA>, val stateClass: KClass<Any>, val initializer: Initializer<Event<*>, Any>, val onEvents: Set<OnEvent<Event<*>, Any>>)

    class SagaContext<S : Any>(val state: S) {
        val commands = mutableListOf<Cmd<*>>()
        var newState: S? = null
        val timeouts = mutableListOf<Timeout>()

        fun dispatch(cmd: Cmd<*>){
            commands.add(cmd)
        }

        fun setState(state: S){
            newState = state
        }
    }

    class InitContext<S: Any> {
        var newState: S? = null
        val commands = mutableListOf<Cmd<*>>()

        fun dispatch(cmd: Cmd<*>){
            commands.add(cmd)
        }

        fun setState(state: S){
            newState = state
        }
    }
}