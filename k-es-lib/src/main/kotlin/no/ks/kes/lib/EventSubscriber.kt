package no.ks.kes.lib

import kotlin.reflect.KClass

interface EventSubscriber {
     fun <E: Event<*>> onEvent(eventClass: KClass<E>, consumer: (E) -> Unit)

}
