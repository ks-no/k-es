package no.ks.kes.serdes.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdSerdes
import no.ks.kes.lib.getSerializationIdAnnotationValue
import kotlin.reflect.KClass

class JacksonCmdSerdes(cmds: Set<KClass<out Cmd<*>>>, private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(Jdk8Module())
        .registerModule(JavaTimeModule())
        .registerModule(KotlinModule.Builder().build())) : CmdSerdes {

    private val commandClasses = cmds.map { getSerializationIdAnnotationValue(it) to it }.toMap()

    override fun deserialize(cmdData: ByteArray, serializationId: String): Cmd<*> =
            objectMapper.readValue(cmdData, commandClasses[serializationId]?.java
                    ?: error("No command with serialization id $serializationId registered"))

    override fun serialize(cmd: Cmd<*>): ByteArray = objectMapper.writeValueAsBytes(cmd)
    override fun getSerializationId(cmdClass: KClass<Cmd<*>>): String =
            getSerializationIdAnnotationValue(cmdClass)

}