package no.ks.kes.lib

interface Event<A : Aggregate> {
    fun upgrade(): Event<A>? = null
}
