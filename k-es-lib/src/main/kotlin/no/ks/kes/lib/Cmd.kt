package no.ks.kes.lib

import java.util.*

interface Cmd<A : Aggregate> {
    fun execute(aggregate: A): List<Event<A>>
    val aggregateId: UUID
    fun initAggregate(): A
    fun useOptimisticLocking(): Boolean = true
}