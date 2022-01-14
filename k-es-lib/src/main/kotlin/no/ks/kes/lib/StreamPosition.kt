package no.ks.kes.lib

sealed class StreamPosition {
    object Start : StreamPosition()
    object End : StreamPosition()
    data class EventNumber(val eventNumberExclusive: Long) : StreamPosition()
}
