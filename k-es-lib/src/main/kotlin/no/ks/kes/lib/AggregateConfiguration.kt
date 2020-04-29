package no.ks.kes.lib

import kotlin.reflect.KClass


interface Aggregate

abstract class AggregateConfiguration<STATE : Aggregate>(val aggregateType: String) {
    protected val applicators: MutableMap<KClass<Event<*>>, (STATE, EventWrapper<*>) -> STATE> = mutableMapOf()
    protected val initializers: MutableMap<KClass<Event<*>>, (EventWrapper<*>) -> STATE> = mutableMapOf()

    protected inline fun <reified E : Event<*>> apply(crossinline applicator: STATE.(E) -> STATE) {
        applicators[E::class as KClass<Event<*>>] = { s, e -> applicator(s, EventUpgrader.upgradeTo(e.event, E::class)) }
    }

    protected inline fun <reified E : Event<*>> init(crossinline initializer: (E) -> STATE) {
        initializers[E::class as KClass<Event<*>>] = { initializer(EventUpgrader.upgradeTo(it.event, E::class)) }
    }

    internal fun getConfiguration(serializationIdFunction: (KClass<Event<*>>) -> String): ValidatedAggregateConfiguration<STATE> =
            ValidatedAggregateConfiguration(
                    aggregateType = aggregateType,
                    applicators = applicators,
                    initializers = initializers,
                    serializationIdFunction = serializationIdFunction
            )

    internal class ValidatedAggregateConfiguration<STATE : Aggregate>(
            val aggregateType: String,
            applicators: Map<KClass<Event<*>>, (STATE, EventWrapper<*>) -> STATE>,
            initializers: Map<KClass<Event<*>>, (EventWrapper<*>) -> STATE>,
            serializationIdFunction: (KClass<Event<*>>) -> String
    ) {
        private val applicators: Map<String, (STATE, EventWrapper<*>) -> STATE>
        private val initializers: Map<String, (EventWrapper<*>) -> STATE>

        init {
            val duplicateApplicators = applicators.keys.map { serializationIdFunction.invoke(it) }.groupBy { it }.filter { it.value.size > 1 }.map { it.key }
            check(duplicateApplicators.isEmpty()) { "There are multiple \"apply\" configurations for the event-types $duplicateApplicators in the configuration for $aggregateType, only a single \"apply\" handler is allowed for each event type" }
            this.applicators = applicators.map { serializationIdFunction.invoke(it.key) to it.value }.toMap()

            val duplicateInitializers = initializers.keys.map { serializationIdFunction.invoke(it) }.groupBy { it }.filter { it.value.size > 1 }.map { it.key }
            check(duplicateApplicators.isEmpty()) { "There are multiple \"init\" configurations for the event-types $duplicateInitializers in the configuration for $aggregateType, only a single \"init\" handler is allowed for each event type" }
            this.initializers = initializers.map { serializationIdFunction.invoke(it.key) to it.value }.toMap()
        }

        internal fun <E : Event<*>> applyEvent(wrapper: EventWrapper<E>, currentState: STATE?): STATE? =
                if (currentState != null) {
                    applicators[wrapper.serializationId]
                            ?.invoke(currentState, wrapper)
                            ?: currentState
                } else {
                    initializers[wrapper.serializationId]
                            ?.invoke(wrapper)
                }
    }
}