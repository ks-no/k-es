package no.ks.kes.lib

import mu.KotlinLogging
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

abstract class Projection {
    protected val projectors: MutableMap<KClass<Event<*>>, (EventWrapper<*>) -> Any?> = mutableMapOf()

    open fun onLive() {
        return
    }

    protected inline fun <reified E : Event<*>> on(crossinline consumer: (E) -> Any?) =
            onWrapper<E> { consumer.invoke(it.event) }

    @Suppress("UNCHECKED_CAST")
    protected inline fun <reified E : Event<*>> onWrapper(crossinline consumer: (EventWrapper<E>) -> Any?) {
        if (AnnotationUtil.isDeprecated(E::class as KClass<Any>))
            error("The projection ${this::class.simpleName} subscribes to event ${E::class.simpleName} which has been deprecated. Please replace with subscription to the new version of this event")

        projectors[E::class as KClass<Event<*>>] = { e ->
            consumer.invoke(e as EventWrapper<E>)
        }
    }

    internal fun getConfiguration(serializationIdFunction: (KClass<Event<*>>) -> String): ValidatedProjectionConfiguration =
            ValidatedProjectionConfiguration(
                    name = this::class.simpleName ?: "anonymous",
                    projectors = projectors,
                    serializationIdFunction = serializationIdFunction
            )

    class ValidatedProjectionConfiguration(
            private val name: String,
            serializationIdFunction: (KClass<Event<*>>) -> String,
            projectors: Map<KClass<Event<*>>, (EventWrapper<*>) -> Any?>
    ) {
        private val projectors: Map<String, (EventWrapper<*>) -> Any?>

        init {
            val duplicateProjectors = projectors.keys.map { serializationIdFunction.invoke(it) }.groupBy { it }.filter { it.value.size > 1 }.map { it.key }
            check(duplicateProjectors.isEmpty()) { "There are multiple \"project\" configurations for the event-types $duplicateProjectors in the projection $name, only a single \"project\" handler is allowed for each event type" }
            this.projectors = projectors.map { serializationIdFunction.invoke(it.key) to it.value }.toMap()
        }

        fun accept(wrapper: EventWrapper<*>) {
            val projector = projectors[wrapper.serializationId]

            if (projector != null) {
                projector.invoke(wrapper)
                log.info("Event ${wrapper.serializationId} on aggregate ${wrapper.event.aggregateId} " +
                        "applied to projection $name")
            }

        }
    }
}