package no.ks.kes.lib

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.util.*

class EventMetadata private constructor(val aggregateId: UUID? = null) {

    companion object {
        val objectMapper: ObjectMapper = ObjectMapper()
            .registerModule(Jdk8Module())
            .registerModule(JavaTimeModule())
            .registerModule(KotlinModule())

        fun fromJson(json: ByteArray): EventMetadata {
            val metaData = String(json)
            if (!metaData.isNullOrEmpty() ) {
                return objectMapper.readValue(json, EventMetadata::class.java)
            } else {
                return EventMetadata()
            }
        }
    }

    data class Builder(
        var aggregateId: UUID? = null
    ) {

        fun aggregateId(aggregateId: UUID) = apply { this.aggregateId = aggregateId }
        fun build() = EventMetadata(aggregateId)
    }

    fun serialize(): String {
        return objectMapper.writeValueAsString(this)
    }
}


