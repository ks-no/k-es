package no.ks.kes.sagajdbc

data class EventWrapper<T : Event<*>>(val event: T, val eventNumber: Long)