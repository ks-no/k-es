package no.ks.kes.serdes.proto

import com.google.protobuf.Message
import no.ks.kes.lib.EventMetadata

interface ProtoEventDeserializer {
    fun deserialize(msg: Message): ProtoEvent<*>
}