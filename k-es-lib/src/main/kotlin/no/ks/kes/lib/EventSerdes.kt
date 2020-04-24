package no.ks.kes.lib

import kotlin.reflect.KClass

interface EventSerdes {
    fun deserialize(eventData: ByteArray, eventType: String): Event<*>
    fun serialize(event: Event<*>): ByteArray
    fun <T : Event<*>> getSerializationId(eventClass: KClass<T>): String
}