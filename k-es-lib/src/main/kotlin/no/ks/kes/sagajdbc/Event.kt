package no.ks.kes.sagajdbc

import java.time.Instant
import java.util.*

interface Event<A: Aggregate> {
    val aggregateId: UUID
    val timestamp: Instant
    fun upgrade(): Event<A>? = null
}
