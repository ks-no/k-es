package no.ks.kes.lib

import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation

object AnnotationUtil {
    fun <T : Any> getSerializationId(event: KClass<T>): String =
            event.findAnnotation<SerializationId>()
                    ?.value
                    ?: error("The class ${event.simpleName} is not annotated with @SerializationId")


    fun <T : Any> isDeprecated(it: KClass<T>): Boolean {
        return it.findAnnotation<Deprecated>() != null
    }


}


val <T: Event<*>> KClass<T>.serializationId: String
    get() = AnnotationUtil.getSerializationId(this)

val <T: Event<*>> KClass<T>.deprecated: Boolean
    get() = AnnotationUtil.isDeprecated(this)