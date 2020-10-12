package no.ks.kes.lib

import kotlin.reflect.KClass

interface EventSubscriberFactory<S: EventSubscription> {
    fun getSerializationId(eventClass: KClass<Event<*>>): String
    fun createSubscriber(subscriber: String, fromEvent: Long, onEvent: (EventWrapper<Event<*>>) -> Unit, onClose: (Exception) -> Unit = {}, onLive: () -> Unit = {}): S
}
