package no.ks.kes.lib

import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation

object AnnotationUtil {
    fun <E : Event<*>> getEventType(event: KClass<E>): String =
            event.findAnnotation<EventType>()
                    ?.value
                    ?: error("The event ${event.simpleName} is not annotated with @EventType")

    fun <S : Saga<*>> getSagaType(saga: KClass<S>): String =
            saga.findAnnotation<SagaName>()
                    ?.value
                    ?: error("The saga ${saga.simpleName} is not annotated with @SagaName")
}