package no.ks.kes.lib

import java.time.Instant
import java.util.*

interface Event<A : Aggregate> {
    val aggregateId: UUID
    val timestamp: Instant
    fun upgrade(): Event<A>? = null
}
