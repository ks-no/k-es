package no.ks.kes.mongodb

import com.mongodb.client.MongoCollection
import org.bson.Document
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoDatabaseUtils
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.SessionSynchronization
import org.springframework.transaction.support.TransactionTemplate

class MongoDBTransactionAwareCollectionFactory(private val factory: MongoDatabaseFactory) {

    fun getTransactionTemplate() = TransactionTemplate(getMongoTransactionManager())

    fun getMongoTransactionManager() = MongoTransactionManager(factory)

    fun getCollection(collectionName: String): MongoCollection<Document> =
        MongoDatabaseUtils.getDatabase(factory, SessionSynchronization.ON_ACTUAL_TRANSACTION)
            .getCollection(collectionName)

    fun getCollection(collectionName: String, databaseName: String): MongoCollection<Document> =
        MongoDatabaseUtils.getDatabase(databaseName, factory, SessionSynchronization.ON_ACTUAL_TRANSACTION)
            .getCollection(collectionName)
}