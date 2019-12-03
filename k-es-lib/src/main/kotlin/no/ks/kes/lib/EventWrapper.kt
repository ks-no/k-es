package no.ks.kes.lib

data class EventWrapper<T : Event>(val event: T, val eventNumber: Long)