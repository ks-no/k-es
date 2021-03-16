package no.ks.kes.serdes.proto

import com.google.protobuf.Message
import java.util.*

interface ProtoEventDeserializer {
    fun deserialize(aggregateId: UUID, msg: Message): ProtoEvent<*>
}