package no.ks.kes.lib

interface EventSubscription {

    fun lastProcessedEvent(): Long
}