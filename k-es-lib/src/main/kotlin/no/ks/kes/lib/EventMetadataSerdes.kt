package no.ks.kes.lib

interface EventMetadataSerdes<T : EventMetadata> {
    fun deserialize(eventData: ByteArray): T
    fun serialize(event: EventMetadata): ByteArray
}