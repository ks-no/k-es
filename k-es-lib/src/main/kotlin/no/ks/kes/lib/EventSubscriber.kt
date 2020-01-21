package no.ks.kes.lib

interface EventSubscriber {
    fun subscribe(consumer: (EventWrapper<Event<*>>) -> Unit)
    fun onClose(handler: (Exception) -> Unit)
    fun onLive(handler: () -> Unit)

}
