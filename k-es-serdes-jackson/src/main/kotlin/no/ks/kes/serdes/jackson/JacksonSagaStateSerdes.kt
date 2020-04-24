package no.ks.kes.serdes.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import no.ks.kes.lib.SagaStateSerdes
import kotlin.reflect.KClass

class JacksonSagaStateSerdes(private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(Jdk8Module())
        .registerModule(JavaTimeModule())
        .registerModule(KotlinModule())) : SagaStateSerdes {
    override fun <T : Any> deserialize(sagaData: ByteArray, sagaStateClass: KClass<T>): T =
            objectMapper.readValue(sagaData, sagaStateClass.java)

    override fun serialize(sagaState: Any): ByteArray = objectMapper.writeValueAsBytes(sagaState)

}