package no.ks.kes.lib

interface CmdSerdes<FORMAT> {
    fun deserialize(cmdData: FORMAT, serializationId: String): Cmd<*>
    fun serialize(cmd: Cmd<*>): FORMAT
}