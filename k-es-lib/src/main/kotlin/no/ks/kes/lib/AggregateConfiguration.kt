package no.ks.kes.lib

import java.util.*
import kotlin.reflect.KClass


interface Aggregate

abstract class AggregateConfiguration<STATE : Aggregate>(val aggregateType: String) {
    protected val applicators: MutableMap<KClass<EventData<*>>, (STATE, EventWrapper<*>) -> STATE> = mutableMapOf()
    protected val initializers: MutableMap<KClass<EventData<*>>, (EventWrapper<*>) -> STATE> = mutableMapOf()

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : EventData<*>> apply(crossinline applicator: STATE.(E) -> STATE) {
        applicators[E::class as KClass<EventData<*>>] = { s, e -> applicator(s, e.event.eventData as E) }
    }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : EventData<*>> init(crossinline initializer: (E, UUID) -> STATE) {
        initializers[E::class as KClass<EventData<*>>] = { initializer(it.event.eventData as E, it.event.aggregateId) }
    }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : EventData<*>> applyEvent(crossinline applicator: STATE.(Event<E>) -> STATE) {
        applicators[E::class as KClass<EventData<*>>] = { s, e ->
            applicator(s, e.event as Event<E>)
        }
    }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : EventData<*>> initEvent(crossinline initializer: (Event<E>, UUID) -> STATE) {
        initializers[E::class as KClass<EventData<*>>] = { initializer(it.event as Event<E>, it.event.aggregateId) }
    }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : EventData<*>> applyWrapper(crossinline applicator: STATE.(EventWrapper<E>) -> STATE) {
        applicators[E::class as KClass<EventData<*>>] = { s, e ->
            applicator(s, e as EventWrapper<E>)
        }
    }
    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : EventData<*>> initWrapper(crossinline initializer: (EventWrapper<E>, UUID) -> STATE) {
        initializers[E::class as KClass<EventData<*>>] = { initializer(it as EventWrapper<E>, it.event.aggregateId) }
    }


    fun getConfiguration(serializationIdFunction: (KClass<EventData<*>>) -> String): ValidatedAggregateConfiguration<STATE> =
            ValidatedAggregateConfiguration(
                    aggregateType = aggregateType,
                    applicators = applicators,
                    initializers = initializers,
                    serializationIdFunction = serializationIdFunction
            )
}
class ValidatedAggregateConfiguration<STATE : Aggregate>(
    val aggregateType: String,
    applicators: Map<KClass<EventData<*>>, (STATE, EventWrapper<*>) -> STATE>,
    initializers: Map<KClass<EventData<*>>, (EventWrapper<*>) -> STATE>,
    serializationIdFunction: (KClass<EventData<*>>) -> String
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

    internal fun <E : EventData<*>> applyEvent(wrapper: EventWrapper<E>, currentState: STATE?): STATE? {
        return if (currentState == null) {
            val initializer = initializers[wrapper.serializationId]

            if(initializer == null && applicators.containsKey(wrapper.serializationId))
                error("Error reading ${aggregateType}(${wrapper.event.aggregateId}): event #${wrapper.eventNumber}(${wrapper.serializationId}) is configured as an applicator in the $aggregateType configuration, but the aggregate state has not yet been initialized. Please verify that an init event precedes this event in the event stream, or update your configuration")

            initializer?.invoke(wrapper)
        } else {
            applicators[wrapper.serializationId]
                    ?.invoke(currentState, wrapper)
                    ?: currentState
        }
    }
}