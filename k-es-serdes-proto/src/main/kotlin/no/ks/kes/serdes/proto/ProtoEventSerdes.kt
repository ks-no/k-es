package no.ks.kes.serdes.proto

import com.google.protobuf.Message
import no.ks.kes.lib.Event
import no.ks.kes.lib.EventSerdes
import kotlin.reflect.KClass
import com.google.protobuf.Any
import no.ks.kes.lib.EventMetadata
import no.ks.kes.lib.getSerializationIdAnnotationValue

class ProtoEventSerdes<T:EventMetadata>(private val register: Map<KClass<out ProtoEvent<*>>, Message>, private val protoEventDeserializer: ProtoEventDeserializer<T> ) : EventSerdes<T> {

    val events = register.keys
        .map { getSerializationId(it) to it }
        .toMap()

    override fun deserialize(eventMetadata: T?, eventData: ByteArray, eventType: String): Event<*> {
        if(eventMetadata == null) throw RuntimeException("Metadata er påkrevd for proto events")

        val eventClazz = events[eventType] ?: throw RuntimeException("Fant ikke event for type $eventType")
        val msgClazz = register[eventClazz] ?: throw RuntimeException("Fant ikke Protobuf type for event type $eventType")

        val any = Any.parseFrom(eventData)
        val msg = any.unpack(msgClazz.javaClass)

        return protoEventDeserializer.deserialize(eventMetadata,  msg)
    }

    override fun isJson() : Boolean = false

    override fun serialize(event: Event<*>): ByteArray {
        try {
            if( event is ProtoEvent<*> ){
                val msg = event.msg
                val any = Any.pack(msg)

                return any.toByteArray()
            } else {
                throw RuntimeException("Event må være av type ProtoEvent ${event.javaClass}")
            }

        } catch (e: Exception) {
            throw RuntimeException("Error during serialization of event: $event")
        }
    }

    override fun <T : Event<*>> getSerializationId(eventClass: KClass<T>): String {
        return getSerializationIdAnnotationValue(eventClass)
    }
}