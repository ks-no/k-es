package no.ks.kes.lib

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import kotlin.reflect.KClass

class JacksonEventMetadataSerdes(private val register: Map<KClass<out Event<*>>, KClass<out EventMetadata>>) : EventMetadataSerdes {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(Jdk8Module())
        .registerModule(JavaTimeModule())
        .registerModule(KotlinModule())

    private val typeMap = register
        .map { getSerializationId(it.key) to it.value }
        .toMap()

    override fun deserialize(metadata: ByteArray, eventType: String): EventMetadata {
        return objectMapper.readValue(metadata, typeMap[eventType]?.javaObjectType ?: throw RuntimeException("No class registered for event type $eventType"))
    }

    override fun serialize(metadata: EventMetadata): ByteArray {
        return objectMapper.writeValueAsBytes(metadata)
    }

    private fun <T : Event<*>> getSerializationId(eventClass: KClass<T>): String {
        return getSerializationIdAnnotationValue(eventClass)
    }
}