package no.ks.kes.mongodb.projection

import com.mongodb.client.MongoClient
import no.ks.kes.lib.HwmTrackerRepository
import no.ks.kes.lib.ProjectionRepository
import no.ks.kes.mongodb.hwm.MongoDBServerHwmTrackerRepository

class MongoDBProjectionRepository(mongoClient: MongoClient, hwmDatabaseName: String, initialHwm: Long) : ProjectionRepository {
    override val hwmTracker: HwmTrackerRepository = MongoDBServerHwmTrackerRepository(mongoClient, hwmDatabaseName, initialHwm)

    override fun transactionally(runnable: () -> Unit) {
        runnable.invoke()
    }
}