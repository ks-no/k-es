package no.ks.kes.mongodb
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClients
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.matchers.shouldBe
import no.ks.kes.mongodb.hwm.MongoDBServerHwmTrackerRepository
import org.bson.UuidRepresentation
import org.testcontainers.containers.MongoDBContainer
import kotlin.random.Random

class MongoDBHwmTrackerRepositoryTest: StringSpec({

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

}) {

        companion object {
            private lateinit var hwmTrackerRepository: MongoDBServerHwmTrackerRepository
            private val initialHwm = Random.nextLong(-1, 1)
        }

        override fun beforeTest(testCase: TestCase) {
            super.beforeTest(testCase)
            val mongoDBContainer = MongoDBContainer("mongo:4.4.3")
            mongoDBContainer.start()

            val mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                    .applyConnectionString(ConnectionString(mongoDBContainer.replicaSetUrl))
                    .uuidRepresentation(UuidRepresentation.JAVA_LEGACY)
                    .build()
            )

            hwmTrackerRepository = MongoDBServerHwmTrackerRepository(mongoClient, "database", initialHwm)

        }

}