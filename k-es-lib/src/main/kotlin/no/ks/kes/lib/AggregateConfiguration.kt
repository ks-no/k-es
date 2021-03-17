package no.ks.kes.lib

import java.util.*
import kotlin.reflect.KClass


interface Aggregate

abstract class AggregateConfiguration<STATE : Aggregate>(val aggregateType: String) {
    protected val applicators: MutableMap<KClass<Event<*>>, (STATE, EventWrapper<*>) -> STATE> = mutableMapOf()
    protected val initializers: MutableMap<KClass<Event<*>>, (EventWrapper<*>) -> STATE> = mutableMapOf()

    protected inline fun <reified E : Event<*>> apply(crossinline applicator: STATE.(E) -> STATE) {
        applicators[E::class as KClass<Event<*>>] = { s, e -> applicator(s, e.event as E) }
    }

    protected inline fun <reified E : Event<*>> init(crossinline initializer: (E, UUID) -> STATE) {
        initializers[E::class as KClass<Event<*>>] = { initializer(it.event as E, it.aggregateId) }
    }

    fun getConfiguration(serializationIdFunction: (KClass<Event<*>>) -> String): ValidatedAggregateConfiguration<STATE> =
            ValidatedAggregateConfiguration(
                    aggregateType = aggregateType,
                    applicators = applicators,
                    initializers = initializers,
                    serializationIdFunction = serializationIdFunction
            )
}
class ValidatedAggregateConfiguration<STATE : Aggregate>(
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

    internal fun <E : Event<*>> applyEvent(wrapper: EventWrapper<E>, currentState: STATE?): STATE? {
        return if (currentState == null) {
            val initializer = initializers[wrapper.serializationId]

            if(initializer == null && applicators.containsKey(wrapper.serializationId))
                error("Error reading ${aggregateType}(${wrapper.aggregateId}): event #${wrapper.eventNumber}(${wrapper.serializationId}) is configured as an applicator in the $aggregateType configuration, but the aggregate state has not yet been initialized. Please verify that an init event precedes this event in the event stream, or update your configuration")

            initializer?.invoke(wrapper)
        } else {
            applicators[wrapper.serializationId]
                    ?.invoke(currentState, wrapper)
                    ?: currentState
        }
    }
}