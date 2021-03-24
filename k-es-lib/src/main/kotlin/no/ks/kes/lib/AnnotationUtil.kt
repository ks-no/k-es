package no.ks.kes.lib

import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation

object AnnotationUtil {
    fun <T : Any> isDeprecated(it: KClass<T>): Boolean {
        return it.findAnnotation<Deprecated>() != null
    }
}

val <T : EventData<*>> KClass<T>.deprecated: Boolean
    get() = AnnotationUtil.isDeprecated(this)