package no.ks.kes.sagajdbc

object SagaTable {
    override fun toString(): String = "Saga"

    val correlationId = "correlationId"
    val serializationId = "serializationId"
    val data = "data"
}

object CmdTable {
    override fun toString(): String = "cmd"

    val id = "id"
    val aggregateId = "aggregateId"
    val nextExecution = "nextExecution"
    val retries = "retries"
    val serializationId = "serializationId"
    val error = "error"
    val errorId = "errorId"

    val data = "data"
}

object HwmTable {
    override fun toString(): String = "hwm"

    val hwm = "hwm"
}