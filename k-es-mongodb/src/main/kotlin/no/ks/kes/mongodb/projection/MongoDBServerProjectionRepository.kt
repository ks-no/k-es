package no.ks.kes.mongodb.projection

import com.mongodb.client.MongoClient
import mu.KotlinLogging
import no.ks.kes.lib.HwmTrackerRepository
import no.ks.kes.lib.ProjectionRepository
import no.ks.kes.mongodb.hwm.MongoDBServerHwmTrackerRepository
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.support.TransactionTemplate

private val log = KotlinLogging.logger {  }

class MongoDBProjectionRepository(mongoClient: MongoClient, hwmDatabaseName: String, initialHwm: Long = -1) : ProjectionRepository {
    private val dbFactory = SimpleMongoClientDatabaseFactory(mongoClient, hwmDatabaseName)
    private val database = dbFactory.mongoDatabase
    private val transactionManager = TransactionTemplate(MongoTransactionManager(dbFactory))

    override val hwmTracker: HwmTrackerRepository = MongoDBServerHwmTrackerRepository(database, hwmDatabaseName, initialHwm)

    override fun transactionally(runnable: () -> Unit) {
        transactionManager.execute {
            try {
                runnable.invoke()
            } catch (e: Exception) {
                log.error(e) { "An error was encountered while updating hwm for projections, transaction will be rolled back" }
                throw e
            }
        }
    }
}