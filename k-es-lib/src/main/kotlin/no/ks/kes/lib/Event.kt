package no.ks.kes.lib

import java.util.*

interface Event<A : Aggregate> {
    val aggregateId: UUID
    fun upgrade(): Event<A>? = null
    fun metadata(): EventMetadata? = null
}
