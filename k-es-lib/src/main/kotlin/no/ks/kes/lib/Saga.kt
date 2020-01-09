package no.ks.kes.lib

abstract class Saga<P> {
    private var cmdHandler: CmdHandler? = null

    internal fun setCmdHandler(cmdHandler: CmdHandler){
        this.cmdHandler = cmdHandler
    }

    protected fun <A: Aggregate> dispatch(cmd: Cmd<A>){
        cmdHandler!!.handle(cmd)
    }

    protected inline fun <reified E : Event<*>> on(crossinline consumer: P.(E) -> Unit) {
        onWrapper<E> { consumer.invoke(this, it.event) }
    }

    protected val mutators: MutableMap<String, (EventWrapper<*>, P) -> Any?> = mutableMapOf()

    protected inline fun <reified E : Event<*>> onWrapper(crossinline consumer: P.(EventWrapper<E>) -> Unit) {
        @Suppress("UNCHECKED_CAST")
        mutators[EventUtil.getEventType(E::class)] = { e, p ->
            consumer.invoke(p, e as EventWrapper<E>)
        }
    }

    fun accept(wrapper: EventWrapper<*>, payload: P): P {
        mutators[EventUtil.getEventType(wrapper.event::class)]
                ?.invoke(wrapper, payload)
        log.info("Event ${EventUtil.getEventType(wrapper.event::class)} on aggregate ${wrapper.event.aggregateId} " +
                "received by saga ${this::class.simpleName}")
        return payload
    }

}
