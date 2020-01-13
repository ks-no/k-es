package no.ks.kes.lib

import mu.KotlinLogging
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

abstract class Saga<S> {
    private var cmdHandler: CmdHandler? = null

    protected var initializer: Pair<KClass<Event<*>>, (EventWrapper<*>) -> Pair<String, S>>? = null
    protected val handlers: MutableMap<KClass<Event<*>>, (EventWrapper<*>, S) -> Any?> = mutableMapOf()

    internal fun setCmdHandler(cmdHandler: CmdHandler) {
        this.cmdHandler = cmdHandler
    }

    internal fun getId(event: EventWrapper<*>): String{
        return initializer!!.second.invoke(event).first
    }

    @Suppress("UNCHECKED_CAST")
    internal fun getHandlers(): Map<KClass<Event<*>>, (EventWrapper<*>, Any) -> Any?> {
        if (handlers.isEmpty())
            log.warn {"No handlers defined for saga ${this::class.simpleName}. Maybe you should add some?"}

        return handlers.toMap() as Map<KClass<Event<*>>, (EventWrapper<*>, Any) -> Any?>
    }

    protected fun <A : Aggregate> dispatch(cmd: Cmd<A>) {
        cmdHandler!!.handle(cmd)
    }

    protected inline fun <reified E : Event<*>> initOn(noinline initializer: (E) -> Pair<String, S>) {
        initOnWrapper{w: EventWrapper<E> -> initializer.invoke(w.event)}
    }

    protected inline fun <reified E : Event<*>> on(crossinline handler: S.(E) -> Unit) {
        onWrapper  {w: EventWrapper<E> -> handler.invoke(this, w.event) }
    }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : Event<*>> initOnWrapper(noinline handler: (EventWrapper<E>) -> Pair<String, S>) {
        initializer = E::class as KClass<Event<*>> to  handler as (EventWrapper<*>) -> Pair<String, S>
    }

    protected inline fun <reified E : Event<*>> onWrapper(crossinline handler: S.(EventWrapper<E>) -> Unit) {
        @Suppress("UNCHECKED_CAST")
        handlers[E::class as KClass<Event<*>>] = { e, p ->
            handler.invoke(p, e as EventWrapper<E>)
        }
    }
}
