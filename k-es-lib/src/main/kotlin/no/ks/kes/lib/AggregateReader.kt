package no.ks.kes.lib

import java.util.*

interface AggregateReader {
    fun <E: Event, T : Aggregate<E>>read(aggregateId: UUID, aggregate: T): T
}