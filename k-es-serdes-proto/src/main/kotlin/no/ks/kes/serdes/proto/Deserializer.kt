package no.ks.kes.serdes.proto

import com.google.protobuf.Message
import java.util.*

interface Deserializer {
    fun deserialize(aggregateId: UUID, msg: Message): ProtoEvent<*>
}