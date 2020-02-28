package no.ks.kes.lib

interface ProjectionRepository: TransactionAwareRepository {

    fun updateHwm(currentEvent: Long, consumerName: String)
    fun currentHwm(consumerName: String): Long
}