package no.ks.kes.lib

import kotlin.reflect.KClass

class SagaManager(private val sagaRepo: SagaRepository, eventSub: EventSubscriber, sagas: Set<Saga<*>>) {

    init {
        sagas.forEach { eventSub.onEvent(it.getInitEvent, {e -> initOrRestore(it.getId(e))) }
    }

    private fun initOrRestore(eventClass: KClass<Event<out Aggregate>>, : (EventWrapper<out Event<*>>) -> Pair<String, Any?>): Any {

    }

    private fun initOrRestore(sagaId: String): Any {
       sagaRepo.retreive(sagaId)
    }


    internal fun init(wrapper: EventWrapper<*>): Pair<String, S> {
    return initFunction!!.invoke(wrapper)
}

internal fun accept(wrapper: EventWrapper<*>, state: S): S {
    handlers[EventUtil.getEventType(wrapper.event::class)]
            ?.invoke(wrapper, state)
    log.info("Event ${EventUtil.getEventType(wrapper.event::class)} on aggregate ${wrapper.event.aggregateId} " +
            "received by saga ${this::class.simpleName}")
    return state
}


    interface SagaRepository {

    }