package no.ks.kes.lib

interface EventMetadataSerdes {
    fun deserialize(eventData: ByteArray, eventType: String): EventMetadata
    fun serialize(event: EventMetadata): ByteArray
}