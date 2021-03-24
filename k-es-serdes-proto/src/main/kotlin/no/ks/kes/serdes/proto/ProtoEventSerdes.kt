package no.ks.kes.serdes.proto

import com.google.protobuf.Message
import no.ks.kes.lib.EventData
import no.ks.kes.lib.EventSerdes
import kotlin.reflect.KClass
import com.google.protobuf.Any
import no.ks.kes.lib.getSerializationIdAnnotationValue

class ProtoEventSerdes(private val register: Map<KClass<out ProtoEventData<*>>, Message>, private val protoEventDeserializer: ProtoEventDeserializer ) : EventSerdes {

    val events = register.keys
        .map { getSerializationId(it) to it }
        .toMap()

    override fun deserialize(eventData: ByteArray, eventType: String): EventData<*> {
        val eventClazz = events[eventType] ?: throw RuntimeException("Fant ikke event for type $eventType")
        val msgClazz = register[eventClazz] ?: throw RuntimeException("Fant ikke Protobuf type for event type $eventType")

        val any = Any.parseFrom(eventData)
        val msg = any.unpack(msgClazz.javaClass)

        return protoEventDeserializer.deserialize(msg)
    }

    override fun isJson() : Boolean = false

    override fun serialize(eventData: EventData<*>): ByteArray {
        try {
            if( eventData is ProtoEventData<*> ){
                val msg = eventData.msg
                val any = Any.pack(msg)

                return any.toByteArray()
            } else {
                throw RuntimeException("Event må være av type ProtoEvent ${eventData.javaClass}")
            }

        } catch (e: Exception) {
            throw RuntimeException("Error during serialization of event: $eventData")
        }
    }

    override fun <T : EventData<*>> getSerializationId(eventClass: KClass<T>): String {
        return getSerializationIdAnnotationValue(eventClass)
    }
}