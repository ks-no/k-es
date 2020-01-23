package no.ks.kes.lib

interface EventSubscriber {
    fun addSubscriber(consumerName: String, fromEvent: Long, onEvent: (EventWrapper<Event<*>>) -> Unit, onClose: (Exception) -> Unit = {}, onLive: () -> Unit = {})
}
