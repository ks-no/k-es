package no.ks.kes.lib

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(value = [ "metadata" ])
interface EventWithMetadata<A : Aggregate> : Event<A> {
    val metadata: EventMetadata
}