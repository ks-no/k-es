package no.ks.kes.serdes.proto

import com.google.protobuf.Message

interface ProtoEventDeserializer {
    fun deserialize(msg: Message): ProtoEvent<*>
}