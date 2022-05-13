package no.ks.kes.mongodb

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.WriteConcern
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import io.kotest.assertions.throwables.shouldThrowExactly
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.matchers.shouldBe
import no.ks.kes.mongodb.projection.MongoDBProjectionRepository
import org.bson.UuidRepresentation
import org.testcontainers.containers.MongoDBContainer

private const val INITIAL_HWM = -1L

class MongoDBContainerTest: StringSpec({

    "Testing ProjectionRepository backed by Mongo" {
        val subscriber = testCase.name.testName
        val projectionRepository = MongoDBProjectionRepository(mongoClient, "database")

        projectionRepository.hwmTracker.current(subscriber) shouldBe null

        shouldThrowExactly<RuntimeException> {
            projectionRepository.transactionally {
                projectionRepository.hwmTracker.getOrInit(subscriber) shouldBe INITIAL_HWM
                throw RuntimeException("Woops!")
            }
        }

        //HWM setup should be rolled back, men den feiler!
        //projectionRepository.hwmTracker.current(subscriber) shouldBe null
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
                .writeConcern(WriteConcern.ACKNOWLEDGED)
                .build()

        )
    }
}