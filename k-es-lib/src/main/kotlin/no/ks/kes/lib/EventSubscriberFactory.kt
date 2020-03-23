package no.ks.kes.lib

interface EventSubscriberFactory {
    fun createSubscriber(subscriber: String, fromEvent: Long, onEvent: (EventWrapper<Event<*>>) -> Unit, onClose: (Exception) -> Unit = {}, onLive: () -> Unit = {})
}
