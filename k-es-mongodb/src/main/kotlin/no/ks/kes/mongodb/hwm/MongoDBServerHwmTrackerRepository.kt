package no.ks.kes.mongodb.hwm

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import com.mongodb.client.model.Updates.set
import mu.KotlinLogging
import no.ks.kes.lib.HwmTrackerRepository
import no.ks.kes.mongodb.HwmCollection
import no.ks.kes.mongodb.MongoDBTransactionAwareCollectionFactory
import org.bson.Document
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoDatabaseUtils
import org.springframework.data.mongodb.SessionSynchronization
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Criteria.where
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Query.query
import org.springframework.data.mongodb.core.query.Update

private val log = KotlinLogging.logger {}

class MongoDBServerHwmTrackerRepository(private val factory: MongoDBTransactionAwareCollectionFactory, private val initialHwm: Long) : HwmTrackerRepository {

    companion object {
        private const val HWVM_VALUE_KEY = "value"
    }

    override fun current(subscriber: String): Long? = factory.getCollection(HwmCollection.name).find(eq(subscriber)).first()?.getLong(HWVM_VALUE_KEY)

    override fun getOrInit(subscriber: String) : Long = current(subscriber) ?: initHwm(subscriber).also { log.info { "no hwm found for subscriber $subscriber, initializing subscriber at $initialHwm" } }

    override fun update(subscriber: String, hwm: Long) {
        factory.getCollection(HwmCollection.name).updateOne(eq(subscriber), set(HWVM_VALUE_KEY, hwm), UpdateOptions().upsert(true))
    }

    private fun initHwm(subscriber: String) : Long {
        update(subscriber, initialHwm)
        return initialHwm
    }

}
