package no.ks.kes.mongodb
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.extensions.testcontainers.perSpec
import io.kotest.matchers.shouldBe
import no.ks.kes.mongodb.hwm.MongoDBServerHwmTrackerRepository
import org.bson.UuidRepresentation
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.testcontainers.containers.MongoDBContainer
import kotlin.random.Random

class MongoDBHwmTrackerRepositoryTest: StringSpec() {
    private val mongoDBContainer = MongoDBContainer("mongo:4.4.3")
    private val initialHwm = Random.nextLong(-1, 1)
    private val  hwmTrackerRepository: MongoDBServerHwmTrackerRepository by lazy {
        MongoDBServerHwmTrackerRepository(MongoDBTransactionAwareCollectionFactory(SimpleMongoClientDatabaseFactory(client, "database")), initialHwm)
    }

    private val client: MongoClient by lazy {
        MongoClients.create(
            MongoClientSettings.builder()
                .applyConnectionString(ConnectionString(mongoDBContainer.replicaSetUrl))
                .uuidRepresentation(UuidRepresentation.JAVA_LEGACY)
                .build()
        )
    }

    init {

        listener(mongoDBContainer.perSpec())
        "Test that a subscriber hwm is created if one does not exist" {
            hwmTrackerRepository.getOrInit("some-subscriber") shouldBe initialHwm
        }

        "Test that a subscriber hwm is updated" {
            val hwm = Random.nextLong()
            val subscriber = "some-subscriber"
            hwmTrackerRepository.getOrInit(subscriber)
            hwmTrackerRepository.update(subscriber, hwm)
            hwmTrackerRepository.current(subscriber) shouldBe hwm

        }
    }

}