package no.ks.kes.lib

interface EventSerdes {
    fun deserialize(eventData: ByteArray, eventType: String): Event<*>
    fun serialize(event: Event<*>): ByteArray
}