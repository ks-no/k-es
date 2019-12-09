package no.ks.kes.lib

import java.util.*

interface Cmd<E : Event, A : Aggregate<out E>> {
    fun execute(aggregate: A): List<E>
    val aggregateId: UUID
    fun initAggregate(): A
    fun useOptimisticLocking(): Boolean = true
}