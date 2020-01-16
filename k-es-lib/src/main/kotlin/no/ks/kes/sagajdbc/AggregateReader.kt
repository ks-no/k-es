package no.ks.kes.sagajdbc

import java.util.*

interface AggregateReader {
    fun <A : Aggregate> read(aggregateId: UUID, aggregate: A): A
}