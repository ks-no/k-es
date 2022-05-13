package no.ks.kes.mongodb.saga

import com.mongodb.TransactionOptions
import com.mongodb.WriteConcern
import com.mongodb.client.MongoClient
import com.mongodb.client.model.*
import mu.KotlinLogging
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdSerdes
import no.ks.kes.lib.SagaRepository
import no.ks.kes.lib.SagaStateSerdes
import no.ks.kes.mongodb.*
import no.ks.kes.mongodb.hwm.MongoDBServerHwmTrackerRepository
import org.bson.Document
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.transaction.support.TransactionTemplate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {  }

class MongoDBServerSagaRepository(mongoClient: MongoClient, sagaDatabaseName: String, private val sagaStateSerdes: SagaStateSerdes, private val cmdSerdes: CmdSerdes, initialHwm: Long = -1) : SagaRepository {

    private val dbFactory = SimpleMongoClientDatabaseFactory(mongoClient, sagaDatabaseName)
    private val database = dbFactory.mongoDatabase
    override val hwmTracker = MongoDBServerHwmTrackerRepository(mongoClient, HwmCollection.name, initialHwm)
    private val transactionManager = MongoTransactionManager(dbFactory)

    private val timeoutCollection = database.getCollection(TimeoutCollection.name)
    private val sagaCollection = database.getCollection(SagaCollection.name)
    private val cmdCollection = database.getCollection(CmdCollection.name)
    private val cmdCounterCollection = database.getCollection(CmdCounterCollection.name)


    private fun generateSequence(): Long {
        return cmdCounterCollection.findOneAndUpdate(
            Filters.eq(CmdCounterCollection.id, CmdCounterCollection.name),
            Updates.inc(CmdCounterCollection.cmdCounter, 1),
            FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true))?.getLong(CmdCounterCollection.cmdCounter) ?: 1
    }

    override fun transactionally(runnable: () -> Unit) {
        TransactionTemplate(transactionManager).execute {
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
                Filters.lt(TimeoutCollection.timeout, OffsetDateTime.now(ZoneOffset.UTC))
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
        log.info { "updating sagas: $states" }

        states.filterIsInstance<SagaRepository.Operation.Insert>().map {
            InsertOneModel(Document(
                     Document()
                        .append(SagaCollection.correlationId, it.correlationId.toString())
                        .append(SagaCollection.serializationId, it.serializationId)
                        .append(SagaCollection.data, String(sagaStateSerdes.serialize(it.newState)))))
        }.let { inserts -> sagaCollection.bulkWrite(inserts) }

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
                            .append(SagaCollection.data, String(sagaStateSerdes.serialize(it.newState!!)))))
            }.let { updates -> sagaCollection.bulkWrite(updates) }

        timeoutCollection.bulkWrite(timeoutsToBulkWrite(states))

        cmdCollection.bulkWrite(cmdsToBulkWrite(states))
    }

    private fun cmdsToBulkWrite(states: Set<SagaRepository.Operation> ): List<InsertOneModel<Document>> {
        return states.filterIsInstance<SagaRepository.Operation.SagaUpdate>()
            .flatMap { saga ->
                saga.commands.map {
                    InsertOneModel(
                        Document()
                            .append(CmdCollection.serializationId, cmdSerdes.getSerializationId(it::class as KClass<Cmd<*>>))
                            .append(CmdCollection.id, generateSequence())
                            .append(CmdCollection.aggregateId, it.aggregateId)
                            .append(CmdCollection.retries, 0)
                            .append(CmdCollection.nextExecution, OffsetDateTime.now(ZoneOffset.UTC))
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
                                .append(TimeoutCollection.sagaSerializationId, saga.correlationId)
                                .append(TimeoutCollection.timeoutId, it.timeoutId)
                                .append(TimeoutCollection.timeout, false)
                                .append(TimeoutCollection.timeout, OffsetDateTime.ofInstant(it.triggerAt, ZoneOffset.UTC))
                        )
                }
            }
    }
}