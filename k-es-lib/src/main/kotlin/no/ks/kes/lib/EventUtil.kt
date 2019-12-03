package no.ks.kes.lib

import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation

object EventUtil {
    fun <E: Event> getEventType(aggEvent: KClass<E>): String =
            aggEvent.findAnnotation<EventType>()
                    ?.value
                    ?: throw RuntimeException(String.format("The event %s is not annotated with @EventType", aggEvent.simpleName))


}