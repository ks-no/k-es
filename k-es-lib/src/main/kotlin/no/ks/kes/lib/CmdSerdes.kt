package no.ks.kes.lib

import kotlin.reflect.KClass

interface CmdSerdes {
    fun deserialize(cmdData: ByteArray, serializationId: String): Cmd<*>
    fun serialize(cmd: Cmd<*>): ByteArray
    fun getSerializationId(cmdClass: KClass<Cmd<*>>): String
}