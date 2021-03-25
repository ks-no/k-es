package no.ks.kes.serdes.proto

import com.google.protobuf.Message
import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.EventData

interface ProtoEventData<A : Aggregate> : EventData<A> {
    val msg: Message
}