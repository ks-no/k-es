package no.ks.kes.serdes.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import no.ks.kes.lib.*
import kotlin.reflect.KClass

class JacksonEventSerdes(events: Set<KClass<out EventData<*>>>,
                         private val objectMapper: ObjectMapper = ObjectMapper()
                                 .registerModule(Jdk8Module())
                                 .registerModule(JavaTimeModule())
                                 .registerModule(KotlinModule())
) : EventSerdes {
    private val events = events
            .map { getSerializationId(it) to it }
            .toMap()

    override fun deserialize(eventData: ByteArray, eventType: String): EventData<*> {
        return try {
            objectMapper.readValue(
                eventData,
                events[eventType]
                    ?.javaObjectType
                    ?: throw RuntimeException("No class registered for event type $eventType")
            )
        } catch (e: Exception) {
            throw  RuntimeException("Error during deserialization of eventType $eventType", e)
        }
    }
    override fun serialize(eventData: EventData<*>): ByteArray =
            try {
                objectMapper.writeValueAsBytes(eventData)
            } catch (e: Exception) {
                throw RuntimeException("Error during serialization of event: $eventData")
            }

    override fun <T : EventData<*>> getSerializationId(eventClass: KClass<T>): String {
        return getSerializationIdAnnotationValue(eventClass)
    }
}