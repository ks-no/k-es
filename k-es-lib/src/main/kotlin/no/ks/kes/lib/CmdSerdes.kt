package no.ks.kes.lib

interface CmdSerdes<FORMAT> {
    fun <T: Cmd<*>> deserialize(cmdData: FORMAT, cmdClass: Class<T>): T
    fun serialize(cmd: Cmd<*>): FORMAT
}