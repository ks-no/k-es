package no.ks.kes.lib



interface Aggregate

abstract class AggregateConfiguration<STATE: Aggregate>(val aggregateType: String) {
    protected val applicators: MutableMap<String, (STATE, EventWrapper<*>) -> STATE> = mutableMapOf()
    protected val initializers: MutableMap<String, (EventWrapper<*>) -> STATE> = mutableMapOf()

    protected inline fun <reified E : Event<*>> apply(crossinline applicator: STATE.(E) -> STATE) {
        val serializationId = AnnotationUtil.getSerializationId(E::class)
        check(!applicators.containsKey(serializationId)) {"There are multiple \"apply\" configurations for the event $serializationId in the aggregate configuration ${this::class.simpleName}, only a single \"apply\" handler is allowed for each event type"}

        applicators[serializationId] = {s, e -> applicator(s, EventUpgrader.upgradeTo(e.event, E::class)) }
    }

    protected inline fun <reified E : Event<*>> init(crossinline initializer: (E) -> STATE) {
        val serializationId = AnnotationUtil.getSerializationId(E::class)
        check(!initializers.containsKey(serializationId)) {"There are multiple \"init\" configurations for the event $serializationId in the aggregate configuration ${this::class.simpleName}, only a single \"init\" handler is allowed for each event type"}

        initializers[serializationId] = {initializer(EventUpgrader.upgradeTo(it.event, E::class)) }
    }

    internal fun <E : Event<*>> applyEvent(wrapper: EventWrapper<E>, currentState: STATE?): STATE? =
            if (currentState != null) {
                applicators[AnnotationUtil.getSerializationId(wrapper.event::class)]
                        ?.invoke(currentState, wrapper)
                        ?: currentState
            } else {
                initializers[AnnotationUtil.getSerializationId(wrapper.event::class)]
                        ?.invoke(wrapper)
            }
}