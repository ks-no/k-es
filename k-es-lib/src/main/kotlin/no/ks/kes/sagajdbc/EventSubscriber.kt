package no.ks.kes.sagajdbc

interface EventSubscriber {
     fun onEvent(consumer: (EventWrapper<Event<*>>) -> Unit)

}
