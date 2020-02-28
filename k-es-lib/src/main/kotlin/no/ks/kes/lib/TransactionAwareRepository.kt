package no.ks.kes.lib

interface TransactionAwareRepository {
    fun transactionally(runnable: () -> Unit)
}