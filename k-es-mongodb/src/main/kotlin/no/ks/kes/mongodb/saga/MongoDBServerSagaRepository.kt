package no.ks.kes.mongodb.saga

import com.mongodb.client.model.*
import mu.KotlinLogging
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdSerdes
import no.ks.kes.lib.SagaRepository
import no.ks.kes.lib.SagaStateSerdes
import no.ks.kes.mongodb.*
import no.ks.kes.mongodb.hwm.MongoDBServerHwmTrackerRepository
import org.bson.Document
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {  }

val DATEFORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

class MongoDBServerSagaRepository(private val factory: MongoDBTransactionAwareCollectionFactory, private val sagaStateSerdes: SagaStateSerdes, private val cmdSerdes: CmdSerdes, initialHwm: Long = -1) : SagaRepository {

    override val hwmTracker = MongoDBServerHwmTrackerRepository(factory, initialHwm)
    private val transactionTemplate = factory.getTransactionTemplate()

    private val timeoutCollection get() = factory.getCollection(TimeoutCollection.name)
    private val sagaCollection get() = factory.getCollection(SagaCollection.name)
    private val cmdCollection get() = factory.getCollection(CmdCollection.name)
    private val cmdCounterCollection get() = factory.getCollection(CmdCounterCollection.name)

    init {
        factory.initCollection(TimeoutCollection.name)
        factory.initCollection(SagaCollection.name)
        factory.initCollection(CmdCollection.name)
        factory.initCollection(CmdCounterCollection.name)
    }


    private fun generateSequence(): Long {
        return cmdCounterCollection.findOneAndUpdate(
            Filters.eq(CmdCounterCollection.id, CmdCounterCollection.name),
            Updates.inc(CmdCounterCollection.cmdCounter, 1),
            FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true))?.get(CmdCounterCollection.cmdCounter, Number::class.java)?.toLong() ?: 1
    }

    override fun transactionally(runnable: () -> Unit) {
        transactionTemplate.execute {
            try {
                runnable.invoke()
            } catch (e: Exception) {
                log.error("An error was encountered while retrieving and executing saga-timeouts, transaction will be rolled back", e)
                throw e
            }
        }
    }

    override fun getReadyTimeouts(): SagaRepository.Timeout? {
        return timeoutCollection.find(
            Filters.and(
                Filters.eq(TimeoutCollection.error, false),
                Filters.lt(TimeoutCollection.timeout, DATEFORMAT.format(OffsetDateTime.now(ZoneOffset.UTC)))
            )).limit(1).singleOrNull()?.let {
            SagaRepository.Timeout(
                sagaSerializationId = it.getString(TimeoutCollection.sagaSerializationId),
                sagaCorrelationId = UUID.fromString((it.getString(TimeoutCollection.sagaCorrelationId))),
                timeoutId = it.getString(TimeoutCollection.timeoutId)
            )
        }
    }

    override fun deleteTimeout(timeout: SagaRepository.Timeout) {
        timeoutCollection.deleteOne(
            Filters.and(
                Filters.eq(TimeoutCollection.sagaSerializationId, timeout.sagaSerializationId),
                Filters.eq(TimeoutCollection.sagaCorrelationId, timeout.sagaCorrelationId.toString()),
                Filters.eq(TimeoutCollection.timeoutId, timeout.timeoutId)
            )
        )
    }

    override fun <T : Any> getSagaState(correlationId: UUID, serializationId: String, sagaStateClass: KClass<T>): T? {
        return sagaCollection.find(
            Filters.and(
                Filters.eq(SagaCollection.correlationId, correlationId.toString()),
                Filters.eq(SagaCollection.serializationId, serializationId)
            )).singleOrNull()?.let {
            sagaStateSerdes.deserialize(it.getString(SagaCollection.data).toByteArray(), sagaStateClass)
        }
    }

    override fun update(states: Set<SagaRepository.Operation>) {
        log.info { "updating sagas: ${states.map { it.newState }.toList()}" }

        states.filterIsInstance<SagaRepository.Operation.Insert>().map {
            InsertOneModel(Document(
                     Document()
                        .append(SagaCollection.correlationId, it.correlationId.toString())
                        .append(SagaCollection.serializationId, it.serializationId)
                        .append(SagaCollection.data, String(sagaStateSerdes.serialize(it.newState)))
            ))
        }.let { inserts -> if (inserts.isNotEmpty()) sagaCollection.bulkWrite(inserts) }

        states.filterIsInstance<SagaRepository.Operation.SagaUpdate>()
            .filter { it.newState != null }
            .map {
                UpdateOneModel<Document>(
                    Filters.and(
                        Filters.eq(SagaCollection.correlationId, it.correlationId.toString()),
                        Filters.eq(SagaCollection.serializationId, it.serializationId)
                    ), Document(
                        "\$set", Document()
                            .append(SagaCollection.correlationId, it.correlationId.toString())
                            .append(SagaCollection.serializationId, it.serializationId)
                            .append(SagaCollection.data, String(sagaStateSerdes.serialize(it.newState!!)))
                    )
                )
            }.let { updates -> if (updates.isNotEmpty()) sagaCollection.bulkWrite(updates) }

        timeoutsToBulkWrite(states).let {
            if (it.isNotEmpty()) timeoutCollection.bulkWrite(it)
        }

        cmdsToBulkWrite(states).let {
            if (it.isNotEmpty()) cmdCollection.bulkWrite(it)
        }
    }

    private fun cmdsToBulkWrite(states: Set<SagaRepository.Operation> ): List<InsertOneModel<Document>> {
        return states.flatMap { saga ->
                saga.commands.map {
                    InsertOneModel(
                        Document()
                            .append(CmdCollection.serializationId, cmdSerdes.getSerializationId(it::class as KClass<Cmd<*>>))
                            .append(CmdCollection.id, generateSequence())
                            .append(CmdCollection.aggregateId, it.aggregateId)
                            .append(CmdCollection.retries, 0)
                            .append(CmdCollection.nextExecution, DATEFORMAT.format(OffsetDateTime.now(ZoneOffset.UTC)))
                            .append(CmdCollection.error, false)
                            .append(CmdCollection.data, String(cmdSerdes.serialize(it)))
                    )
                }
            }
    }

    private fun timeoutsToBulkWrite(states: Set<SagaRepository.Operation>) : List<InsertOneModel<Document>> {
        return states.filterIsInstance<SagaRepository.Operation.SagaUpdate>()
            .flatMap { saga ->
                saga.timeouts.map {
                        InsertOneModel(
                            Document()
                                .append(TimeoutCollection.sagaCorrelationId, saga.correlationId.toString())
                                .append(TimeoutCollection.sagaSerializationId, saga.serializationId)
                                .append(TimeoutCollection.timeoutId, it.timeoutId)
                                .append(TimeoutCollection.error, false)
                                .append(TimeoutCollection.timeout, DATEFORMAT.format(OffsetDateTime.ofInstant(it.triggerAt, ZoneOffset.UTC)))
                        )
                }
            }
    }
}