package no.ks.kes.lib

import java.util.*

interface Event {
    val aggregateId: UUID
    val timestamp: Long
    fun upgrade(): Event? = null
}
