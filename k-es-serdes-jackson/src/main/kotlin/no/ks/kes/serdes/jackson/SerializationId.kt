package no.ks.kes.serdes.jackson

import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation

@kotlin.annotation.Retention(AnnotationRetention.RUNTIME)
annotation class SerializationId(val value: String)

fun <T : Any> getSerializationIdAnnotationValue(clazz: KClass<T>): String =
        (clazz.findAnnotation<SerializationId>()
                ?.value
                ?: error("The class ${clazz.simpleName} is not annotated with @SerializationId")
                ).also { check(it.isNotBlank()) { "The class ${clazz.simpleName} is annotated with @SerializationId, but the value is empty" } }