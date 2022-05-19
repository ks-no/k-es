package no.ks.kes.mongodb

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import io.kotest.assertions.asClue
import io.kotest.assertions.throwables.shouldThrowExactly
import io.kotest.assertions.timing.eventually
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.matchers.shouldBe
import no.ks.kes.lib.Sagas
import no.ks.kes.mongodb.projection.MongoDBProjectionRepository
import no.ks.kes.mongodb.saga.MongoDBServerCommandQueue
import no.ks.kes.mongodb.saga.MongoDBServerSagaRepository
import no.ks.kes.serdes.jackson.JacksonSagaStateSerdes
import no.ks.kes.test.example.*
import no.ks.kes.test.withKes
import org.bson.UuidRepresentation
import org.junit.jupiter.api.fail
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.MongoDBContainer
import java.util.*
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

private const val INITIAL_HWM = -1L

@ExperimentalTime
class MongoDBContainerTest: StringSpec({


    "Testing ProjectionRepository backed by Mongo" {
        val subscriber = testCase.name.testName
        val projectionRepository = MongoDBProjectionRepository(MongoDBTransactionAwareCollectionFactory(SimpleMongoClientDatabaseFactory(mongoClient, "database")))

        projectionRepository.hwmTracker.current(subscriber) shouldBe null

        shouldThrowExactly<RuntimeException> {
            projectionRepository.transactionally {
                projectionRepository.hwmTracker.getOrInit(subscriber) shouldBe INITIAL_HWM
                throw RuntimeException("Woops!")
            }
        }

        projectionRepository.hwmTracker.current(subscriber) shouldBe null
    }

    "Test using SagaRepository and CommandQueue backed by Mongo" {
        mongoClient.use { client ->
            withKes(
                eventSerdes = Events.serdes,
                cmdSerdes = Cmds.serdes
            ) { kesTest ->
                val sagaRepository = MongoDBServerSagaRepository(
                    factory = MongoDBTransactionAwareCollectionFactory(SimpleMongoClientDatabaseFactory(mongoClient, "database")),
                    sagaStateSerdes = JacksonSagaStateSerdes(),
                    cmdSerdes = kesTest.cmdSerdes
                )
                val cmdHandler = EngineCmdHandler(repository = kesTest.aggregateRepository)
                val commandQueue = MongoDBServerCommandQueue(
                    mongoClient = client,
                    cmdDatabaseName = "database",
                    cmdSerdes = kesTest.cmdSerdes,
                    cmdHandlers = setOf(cmdHandler)
                )
                Sagas.initialize(eventSubscriberFactory = kesTest.subscriberFactory,
                    sagaRepository = sagaRepository,
                    sagas = setOf(EngineSaga),
                    commandQueue = commandQueue,
                    pollInterval = 1000,
                    onClose = {
                        fail(it)
                    }
                )
                val aggregateId = UUID.randomUUID()
                TransactionTemplate(MongoTransactionManager(SimpleMongoClientDatabaseFactory(client, "database"))).execute {
                    cmdHandler.handle(Cmds.Create(aggregateId))
                }
                eventually(10.seconds) {
                    cmdHandler.handle(Cmds.Check(aggregateId)).asClue {
                        it.startCount shouldBe 1
                        it.running shouldBe false
                    }
                    sagaRepository.getSagaState(aggregateId, SAGA_SERILIZATION_ID, EngineSagaState::class)?.asClue {
                        it.stoppedBySaga shouldBe true
                    } ?: fail { "Ingen saga state funnet for aggregat $aggregateId" }

                }

            }
        }
    }

}) {

    companion object {
        private lateinit var mongoClient: MongoClient
    }

    override fun beforeTest(testCase: TestCase) {
        super.beforeTest(testCase)
        val mongoDBContainer = MongoDBContainer("mongo:4.4.3")
        mongoDBContainer.start()

        mongoClient = MongoClients.create(
            MongoClientSettings.builder()
                .applyConnectionString(ConnectionString(mongoDBContainer.replicaSetUrl))
                .uuidRepresentation(UuidRepresentation.JAVA_LEGACY)
                .build()

        )
    }
}