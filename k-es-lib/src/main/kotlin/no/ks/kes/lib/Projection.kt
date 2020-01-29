package no.ks.kes.lib

import mu.KotlinLogging
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

abstract class Projection {
    protected val projectors: MutableMap<String, (EventWrapper<*>) -> Any?> = mutableMapOf()

    open fun onLive() {
        return
    }

    protected inline fun <reified E : Event<*>> on(crossinline consumer: (E) -> Any?) =
            onWrapper<E> { consumer.invoke(it.event) }

    protected inline fun <reified E : Event<*>> onWrapper(crossinline consumer: (EventWrapper<E>) -> Any?) {
        if (AnnotationUtil.isDeprecated(E::class as KClass<Any>))
            error("The projection ${this::class.simpleName} subscribes to event ${E::class.simpleName} which has been deprecated. Please replace with subscription to the new version of this event")

        @Suppress("UNCHECKED_CAST")
        projectors[AnnotationUtil.getSerializationId(E::class)] = { e ->
            consumer.invoke(e as EventWrapper<E>)
        }
    }

    fun accept(wrapper: EventWrapper<*>) {
        projectors[AnnotationUtil.getSerializationId(wrapper.event::class)]
                ?.invoke(wrapper)
        log.info("Event ${AnnotationUtil.getSerializationId(wrapper.event::class)} on aggregate ${wrapper.event.aggregateId} " +
                "received by projection ${this::class.simpleName}")
    }
}