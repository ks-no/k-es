package no.ks.kes.lib

import kotlin.reflect.KClass

interface EventSubscriberFactory<S: EventSubscription> {
    fun getSerializationId(eventDataClass: KClass<EventData<*>>): String
    fun createSubscriber(hwmId: String, fromEvent: Long, onEvent: (EventWrapper<EventData<*>>) -> Unit, onError: (Exception) -> Unit = {}, onLive: () -> Unit = {}): S
}
