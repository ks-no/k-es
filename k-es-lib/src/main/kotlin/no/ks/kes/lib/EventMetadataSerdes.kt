package no.ks.kes.lib

interface EventMetadataSerdes<T : EventMetadata> {
    fun deserialize(data: ByteArray): T
    fun serialize(metadata: EventMetadata): ByteArray
}