package no.ks.kes.lib

import kotlin.reflect.KClass

interface EventSerdes {
    fun deserialize(eventData: ByteArray, eventType: String): EventData<*>
    fun serialize(eventData: EventData<*>): ByteArray
    fun <T : EventData<*>> getSerializationId(eventClass: KClass<T>): String
    fun isJson() : Boolean = true
}