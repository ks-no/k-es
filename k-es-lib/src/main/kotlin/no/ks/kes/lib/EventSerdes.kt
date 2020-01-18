package no.ks.kes.lib

interface EventSerdes<FORMAT> {
    fun deserialize(eventData: FORMAT, eventType: String): Event<*>
    fun serialize(event: Event<*>): FORMAT
}