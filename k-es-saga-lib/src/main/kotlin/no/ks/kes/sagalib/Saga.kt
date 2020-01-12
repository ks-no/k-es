package no.ks.kes.sagalib

import mu.KotlinLogging
import no.ks.kes.lib.*
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

abstract class Saga<S> {
    private var cmdHandler: CmdHandler? = null
    protected var initializer: Pair<KClass<Event<*>>, (e: Event<*>) -> Pair<String, S>>? = null

    internal fun setCmdHandler(cmdHandler: CmdHandler) {
        this.cmdHandler = cmdHandler
    }

    protected fun <A : Aggregate> dispatch(cmd: Cmd<A>) {
        cmdHandler!!.handle(cmd)
    }

    protected inline fun <reified E : Event<*>> createOn(noinline initializer: (E) -> Pair<String, S>) {
        @Suppress("UNCHECKED_CAST")
        this.initializer = E::class as KClass<Event<*>> to initializer as (Event<*>) -> Pair<String, S>
    }

    protected inline fun <reified E : Event<*>> on(crossinline handler: S.(E) -> Unit) {
        onWrapper<E> { handler.invoke(this, it.event) }
    }

    protected val handlers: MutableMap<String, (EventWrapper<*>, S) -> Any?> = mutableMapOf()

    protected inline fun <reified E : Event<*>> initOnWrapper(crossinline handler: S.(EventWrapper<E>) -> S) {
        @Suppress("UNCHECKED_CAST")
        handlers[EventUtil.getEventType(E::class)] = { e, p ->
            handler.invoke(p, e as EventWrapper<E>)
        }
    }

    protected inline fun <reified E : Event<*>> onWrapper(crossinline handler: S.(EventWrapper<E>) -> Unit) {
        @Suppress("UNCHECKED_CAST")
        handlers[EventUtil.getEventType(E::class)] = { e, p ->
            handler.invoke(p, e as EventWrapper<E>)
        }
    }

    fun accept(wrapper: EventWrapper<*>, state: S): S {
        handlers[EventUtil.getEventType(wrapper.event::class)]
                ?.invoke(wrapper, state)
        log.info("Event ${EventUtil.getEventType(wrapper.event::class)} on aggregate ${wrapper.event.aggregateId} " +
                "received by saga ${this::class.simpleName}")
        return state
    }

}
