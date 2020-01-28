package no.ks.kes.sagajdbc

object SagaTable {
    override fun toString(): String = "saga"

    val correlationId = "correlationId"
    val serializationId = "serializationId"
    val data = "data"
}

object TimeoutTable {
    override fun toString(): String = "timeout"

    val sagaCorrelationId = "sagaCorrelationId"
    val sagaSerializationId = "sagaSerializationId"
    val timeoutId = "timeoutId"
    val timeout = "timeout"
    val error = "error"
    val errorId = "errorId"
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

    val sagaHwm = "sagaHwm"
}