package no.ks.kes.serdes.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import no.ks.kes.lib.AnnotationUtil
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdSerdes
import kotlin.reflect.KClass

class JacksonCmdSerdes(cmds: Set<KClass<out Cmd<*>>>, private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(Jdk8Module())
        .registerModule(JavaTimeModule())
        .registerModule(KotlinModule())) : CmdSerdes<String> {

    val commandClasses = cmds.map { AnnotationUtil.getSerializationId(it) to it }.toMap()

    override fun deserialize(cmdData: String, serializationId: String): Cmd<*> =
            objectMapper.readValue(cmdData, commandClasses[serializationId]?.java ?: error("No command with serialization id $serializationId registered"))

    override fun serialize(cmd: Cmd<*>): String = objectMapper.writeValueAsString(cmd)

}