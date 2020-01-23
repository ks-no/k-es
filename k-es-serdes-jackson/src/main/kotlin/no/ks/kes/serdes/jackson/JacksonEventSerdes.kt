package no.ks.kes.serdes.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import no.ks.kes.lib.AnnotationUtil
import no.ks.kes.lib.Event
import no.ks.kes.lib.EventSerdes
import kotlin.reflect.KClass

class JacksonEventSerdes(events: Set<KClass<out Event<*>>>,
                         private val objectMapper: ObjectMapper = ObjectMapper()
                                 .registerModule(Jdk8Module())
                                 .registerModule(JavaTimeModule())
                                 .registerModule(KotlinModule())
) : EventSerdes<String> {
    private val events = events
            .map { AnnotationUtil.getSerializationId(it) to it }
            .toMap()

    override fun deserialize(eventData: String, eventType: String): Event<*> =
            try {
                objectMapper.readValue(
                        eventData,
                        events[eventType]
                                ?.javaObjectType
                                ?: throw RuntimeException("No class registered for event type $eventType"))
            } catch (e: Exception) {
                throw  RuntimeException("Error during deserialization of eventType $eventType", e)
            }

    override fun serialize(event: Event<*>): String =
            try {
                String(objectMapper.writeValueAsBytes(event))
            } catch (e: Exception) {
                throw RuntimeException("Error during serialization of event: $event")
            }
}