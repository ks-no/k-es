package no.ks.kes.jdbc

object SagaTable : Table(){
    override val tableName = "saga"
    const val correlationId = "correlationId"
    const val serializationId = "serializationId"
    const val data = "data"
}

object TimeoutTable : Table(){
    override val tableName = "timeout"
    const val sagaCorrelationId = "sagaCorrelationId"
    const val sagaSerializationId = "sagaSerializationId"
    const val timeoutId = "timeoutId"
    const val timeout = "timeout"
    const val error = "error"
    const val errorId = "errorId"
}

object CmdTable : Table(){
    override val tableName = "cmd"
    const val id = "id"
    const val aggregateId = "aggregateId"
    const val nextExecution = "nextExecution"
    const val retries = "retries"
    const val serializationId = "serializationId"
    const val error = "error"
    const val errorId = "errorId"
    const val data = "data"
}

object HwmTable : Table(){
    override val tableName = "hwm"
    const val subscriber = "subscriber"
    const val hwm = "hwm"
}

abstract class Table{
    protected abstract val tableName: String
    fun qualifiedName(schema: String?): String =
            if (schema != null) "$schema.$tableName" else tableName
}