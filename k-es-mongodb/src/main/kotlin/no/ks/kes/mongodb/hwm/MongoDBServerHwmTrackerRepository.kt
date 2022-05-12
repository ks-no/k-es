package no.ks.kes.mongodb.hwm

import com.mongodb.client.MongoClient
import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import mu.KotlinLogging
import no.ks.kes.lib.HwmTrackerRepository
import no.ks.kes.mongodb.HwmCollection

private val log = KotlinLogging.logger {}

class MongoDBServerHwmTrackerRepository(mongoClient: MongoClient, hwmDatabaseName: String, private val initialHwm: Long) : HwmTrackerRepository {

    private val database = mongoClient.getDatabase(hwmDatabaseName)
    private val hwmCollection = database.getCollection(HwmCollection.name)

    companion object {
        private const val HWVM_VALUE_KEY = "value"
    }

    override fun current(subscriber: String): Long? = hwmCollection.find(Filters.eq(subscriber)).first()?.getLong(HWVM_VALUE_KEY)

    override fun getOrInit(subscriber: String) : Long = current(subscriber) ?: initHwm(subscriber).also { log.info { "no hwm found for subscriber $subscriber, initializing subscriber at $initialHwm" } }

    override fun update(subscriber: String, hwm: Long) {

        hwmCollection.updateOne(Filters.eq(subscriber), Updates.set(HWVM_VALUE_KEY, hwm), UpdateOptions().upsert(true))
            .also { log.debug { "Updating hwm for $subscriber to $hwm" } }
    }

    private fun initHwm(subscriber: String) : Long {
        update(subscriber, initialHwm)
        return initialHwm
    }

}
