package no.ks.kes.mongodb

object SagaCollection {
    const val name = "saga"
    const val correlationId = "correlationId"
    const val serializationId = "serializationId"
    const val data = "data"
}

object TimeoutCollection {
    const val name = "timeout"
    const val sagaCorrelationId = "sagaCorrelationId"
    const val sagaSerializationId = "sagaSerializationId"
    const val timeoutId = "timeoutId"
    const val timeout = "timeout"
    const val error = "error"
}

object CmdCollection {
    const val name = "cmd"
    const val id = "_id"
    const val aggregateId = "aggregateId"
    const val nextExecution = "nextExecution"
    const val retries = "retries"
    const val serializationId = "serializationId"
    const val error = "error"
    const val errorId = "errorId"
    const val data = "data"
}

object HwmCollection {
    const val name = "hwm"
}

object CmdCounterCollection {
    const val name = "cmdCounter"
    const val id = "_id"
    const val cmdCounter = "count"
}