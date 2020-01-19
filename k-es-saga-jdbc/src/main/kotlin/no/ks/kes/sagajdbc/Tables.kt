package no.ks.kes.sagajdbc

object SagaTable {
    val name = "saga"

    val correlationId = "correlationId"
    val serializationId = "serializationId"
    val data = "data"
}

object CmdTable {
    val name = "cmd"

    val id = "id"
    val serializationId = "serializationId"
    val data = "data"
}

object HwmTable {
    val name = "hwm"

    val hwm = "hwm"
}