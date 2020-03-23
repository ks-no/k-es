package no.ks.kes.lib

interface TransactionalRepository {
    fun transactionally(runnable: () -> Unit)
}