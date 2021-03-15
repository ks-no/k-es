package no.ks.kes.serdes.proto

import com.google.protobuf.Message
import no.ks.kes.lib.Event
import no.ks.kes.lib.EventSerdes
import kotlin.reflect.KClass
import com.google.protobuf.Any
import no.ks.kes.lib.EventMeta

class ProtoEventSerdes(private val register: Map<KClass<out ProtoEvent<*>>, Message> ) : EventSerdes {

    private val events = register.keys
        .map { getSerializationId(it) to it }
        .toMap()

    override fun deserialize(eventMeta: EventMeta, eventData: ByteArray, eventType: String): Event<*> {
        val eventClazz = events[eventType] ?: throw RuntimeException("Fant ikke event for type $eventType")
        val msgClazz = register[eventClazz] ?: throw RuntimeException("Fant ikke Protobuf type for event type $eventType")

        val any = Any.parseFrom(eventData)
        val msg = any.unpack(msgClazz.javaClass)

        for (constructor in eventClazz.constructors) {
            try {
                return constructor.call(eventMeta.aggregateId ?: throw RuntimeException("Mangler aggregateId for event $eventType"), msg)
            } catch (exception: Exception){
                throw RuntimeException("Feil ved opprettelse av $eventClazz")
            }
        }
        throw RuntimeException("Event $eventClazz hadde ikke implementert konstruktør som tok inn param av type Protobuf Message")
    }

    override fun isJson() : Boolean = false

    override fun serialize(event: Event<*>): ByteArray {
        try {
            if( event is ProtoEvent ){
                val msg = event.getMsg()
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