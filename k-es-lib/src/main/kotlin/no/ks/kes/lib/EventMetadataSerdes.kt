package no.ks.kes.lib

interface EventMetadataSerdes<T : Metadata> {
    fun deserialize(data: ByteArray): T
    fun serialize(metadata: Metadata): ByteArray
}