package no.ks.kes.serdes.proto

import com.google.protobuf.Message
import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.EventWithMetadata

interface ProtoEvent<A : Aggregate> : EventWithMetadata<A> {
    fun getMsg() : Message
}