package no.ks.kes.lib

import kotlin.reflect.KClass

interface EventSubscriberFactory<S: EventSubscription> {
    fun getSerializationId(eventDataClass: KClass<EventData<*>>): String
    fun createSubscriber(subscriber: String, fromEvent: Long, onEvent: (EventWrapper<EventData<*>>) -> Unit, onClose: (Exception) -> Unit = {}, onLive: () -> Unit = {}): S
}
