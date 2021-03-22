package no.ks.kes.lib

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import kotlin.reflect.KClass

class JacksonEventMetadataSerdes<T : Metadata>(val clazz: KClass<T>): EventMetadataSerdes<T> {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(Jdk8Module())
        .registerModule(JavaTimeModule())
        .registerModule(KotlinModule())

    override fun deserialize(metadata: ByteArray): T {
        return objectMapper.readValue(metadata, clazz.javaObjectType)
    }

    override fun serialize(metadata: Metadata): ByteArray {
        return objectMapper.writeValueAsBytes(metadata)
    }
}