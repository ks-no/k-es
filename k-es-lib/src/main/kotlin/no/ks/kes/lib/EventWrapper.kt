package no.ks.kes.lib

data class EventWrapper<T : EventData<*>>(
        val event: Event<T>,
        val eventNumber: Long,
        val serializationId: String
)