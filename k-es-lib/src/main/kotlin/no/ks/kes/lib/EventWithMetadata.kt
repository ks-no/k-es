package no.ks.kes.lib

interface EventWithMetadata<A : Aggregate> : Event<A> {
    val metadata: EventMetadata
}