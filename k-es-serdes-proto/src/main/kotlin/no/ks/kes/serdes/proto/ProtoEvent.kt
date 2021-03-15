package no.ks.kes.serdes.proto

import com.google.protobuf.Message
import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.Event

interface ProtoEvent<A : Aggregate> : Event<A> {
    fun getMsg() : Message
}