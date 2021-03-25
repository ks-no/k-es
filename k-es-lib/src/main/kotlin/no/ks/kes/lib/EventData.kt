package no.ks.kes.lib

interface EventData<A : Aggregate> {
    fun upgrade(): EventData<A>? = null
}
