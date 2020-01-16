package no.ks.kes.lib

import mu.KotlinLogging
import java.util.*
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

abstract class Saga<STATE : Any>(private val stateClass: KClass<STATE>) {


    protected var initializer: MutableList<Initializer<*, STATE>> = mutableListOf()
    protected val onEvents: MutableList<OnEvent<*, STATE>> = mutableListOf()



    internal fun setCmdHandler(cmdHandler: CmdHandler) {
        this.commandSink()
    }

    fun getConfiguration(): SagaConfiguration<*> {
        val duplicateEvents = (onEvents.map { it.eventClass } + initializer.map { it.eventClass })
                .groupingBy { it }
                .eachCount()
                .filter { it.value > 1 }

        when {
            initializer.isEmpty() -> error("No \"initOn\" defined in saga ${this::class.simpleName}. Please define an initializer")
            initializer.size > 1 -> error("Multiple \"initOn\" in saga ${this::class.simpleName}. Please specify a single initializer")
            onEvents.isEmpty() -> log.warn {"No handlers specified in saga ${this::class.simpleName}. Consider adding some?"}
            duplicateEvents.isNotEmpty() -> error("The following events occur multiple times in the \"initOn\" or \"on\" specification in saga ${this::class.simpleName}. Please remove duplicates")
        }

        @Suppress("UNCHECKED_CAST")
        return SagaConfiguration(
                this::class,
                stateClass as KClass<Any>,
                initializer.single() as Initializer<Event<*>, Any?>,
                onEvents.toSet() as Set<OnEvent<Event<*>, Any>>
        )
    }

    protected inline fun <reified E : Event<*>> initOn(crossinline correlationId: E.() -> UUID, crossinline initializer: InitContext.(E) -> STATE?) =
            initOnWrapper({ correlationId.invoke(it.event) }, { w: EventWrapper<E> -> initializer.invoke(this, w.event) })

    protected inline fun <reified E : Event<*>> on(crossinline correlationId: (E) -> UUID, crossinline handler: SagaContext<STATE>.(E) -> STATE?) =
            onWrapper({ correlationId.invoke(this.event) }, { w: EventWrapper<E> -> handler.invoke(this, w.event) })

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : Event<*>> initOnWrapper(crossinline correlationId: (EventWrapper<E>) -> UUID, noinline handler: InitContext.(EventWrapper<E>) -> STATE?) {
            initializer.add(Initializer(E::class as KClass<Event<*>>, { correlationId.invoke(it as EventWrapper<E>) }, { e, p -> handler.invoke(p, e as EventWrapper<E>) }))
    }

    protected inline fun <reified E : Event<*>> onWrapper(crossinline correlationId: EventWrapper<E>.() -> UUID, crossinline handler: SagaContext<STATE>.(EventWrapper<E>) -> STATE?) {
        onEvents.add(OnEvent(E::class, { correlationId.invoke(it) }, { e, p -> handler.invoke(p, e) }))
    }

    data class OnEvent<E : Event<*>, S: Any>(val eventClass: KClass<E>, val correlationId: (EventWrapper<E>) -> UUID, val handler: (e: EventWrapper<E>, s: SagaContext<S>) -> S?)
    data class Initializer<E : Event<*>, S>(val eventClass: KClass<E>, val correlationId: (EventWrapper<E>) -> UUID, val handler: (e: EventWrapper<E>, InitContext) -> S?)
    data class SagaConfiguration<SAGA: Saga<*>>(val sagaClass: KClass<SAGA>, val stateClass: KClass<Any>, val initializer: Initializer<Event<*>, Any?>, val onEvents: Set<OnEvent<Event<*>, Any>>)

    class SagaContext<S: Any>(val state: S){
        val commands = mutableListOf<Cmd<*>>()
    }

    class InitContext {
        val commands = mutableListOf<Cmd<*>>()
    }
}