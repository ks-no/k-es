package no.ks.kes.serdes.proto

import com.google.protobuf.Message
import no.ks.kes.lib.EventMetadata

interface ProtoEventDeserializer<T : EventMetadata> {
    fun deserialize(metadata: T, msg: Message): ProtoEvent<*>
}