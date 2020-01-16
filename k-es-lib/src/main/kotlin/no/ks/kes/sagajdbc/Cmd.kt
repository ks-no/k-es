package no.ks.kes.sagajdbc

import java.util.*

interface Cmd<A : Aggregate> {
    fun execute(aggregate: A): List<Event<A>>
    val aggregateId: UUID
    fun initAggregate(): A
    fun useOptimisticLocking(): Boolean = true
}