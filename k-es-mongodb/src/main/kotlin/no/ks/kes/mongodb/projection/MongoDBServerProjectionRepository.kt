package no.ks.kes.mongodb.projection

import mu.KotlinLogging
import no.ks.kes.lib.HwmTrackerRepository
import no.ks.kes.lib.ProjectionRepository
import no.ks.kes.mongodb.MongoDBTransactionAwareCollectionFactory
import no.ks.kes.mongodb.hwm.MongoDBServerHwmTrackerRepository

private val log = KotlinLogging.logger { }

open class MongoDBProjectionRepository(factory: MongoDBTransactionAwareCollectionFactory, initialHwm: Long = -1) : ProjectionRepository {

    private val transactionTemplate = factory.getTransactionTemplate()

    override val hwmTracker: HwmTrackerRepository = MongoDBServerHwmTrackerRepository(factory, initialHwm)

    override fun transactionally(runnable: () -> Unit) {
        transactionTemplate.execute {
            try {
                runnable.invoke()
            } catch (e: Exception) {
                log.error(e) { "An error was encountered while updating hwm for projections, transaction will be rolled back" }
                throw e
            }
        }
    }
}