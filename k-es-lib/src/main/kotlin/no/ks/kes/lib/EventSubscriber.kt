package no.ks.kes.lib

interface EventSubscriber {
    fun addSubscriber(consumerName: String, consumer: (EventWrapper<Event<*>>) -> Unit)
    fun onClose(handler: (Exception) -> Unit)
    fun onLive(handler: () -> Unit)
}
