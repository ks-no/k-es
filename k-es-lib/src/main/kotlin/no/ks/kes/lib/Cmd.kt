package no.ks.kes.lib

import java.util.*

interface Cmd<A : Aggregate> {
    val aggregateId: UUID
    fun useOptimisticLocking(): Boolean = true
}