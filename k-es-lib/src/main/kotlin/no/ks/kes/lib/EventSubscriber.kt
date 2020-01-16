package no.ks.kes.lib

interface EventSubscriber {
     fun subscribe(fromEvent: Long, consumer: (EventWrapper<Event<*>>) -> Unit)

}
