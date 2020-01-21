package no.ks.kes.lib

import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation

object AnnotationUtil {
    fun <T : Any> getSerializationId(event: KClass<T>): String =
            event.findAnnotation<SerializationId>()
                    ?.value
                    ?: error("The class ${event.simpleName} is not annotated with @SerializationId")

    fun <T : Any> getAggregateType(event: KClass<T>): String =
            event.findAnnotation<AggregateType>()
                    ?.value
                    ?: error("The class ${event.simpleName} is not annotated with @AggregateType")
}