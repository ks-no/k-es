package no.ks.kes.sagajdbc

interface EventSerdes {
    fun deserialize(eventData: ByteArray, eventType: String): Event<*>
    fun serialize(event: Event<*>): ByteArray
}