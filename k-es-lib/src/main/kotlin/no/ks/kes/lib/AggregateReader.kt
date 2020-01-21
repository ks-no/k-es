package no.ks.kes.lib

import java.util.*

interface AggregateReader {
    fun <A : Aggregate> read(aggregateId: UUID, aggregate: A): A?
}