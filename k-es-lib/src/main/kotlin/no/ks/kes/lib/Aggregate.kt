package no.ks.kes.lib

abstract class Aggregate {
    var applicators: MutableMap<String, (Aggregate, Event<*>) -> Aggregate> = mutableMapOf()
        private set
    var currentEventNumber: Long = 0
        internal set

    inline fun <reified E : Event<*>> on(crossinline consumer: (E) -> Unit) {
        applicators[AnnotationUtil.getEventType(E::class)] =
                { a, e -> consumer(EventUpgrader.upgradeTo(e, E::class)); a }
    }

    abstract val aggregateType: String
}

fun <A : Aggregate> A.withCurrentEventNumber(currentEventNumber: Long): A =
        apply { this.currentEventNumber = currentEventNumber }

@Suppress("UNCHECKED_CAST")
fun <E : Event<A>, A : Aggregate> A.applyEvent(event: E, eventNumber: Long): A =
        applicators[AnnotationUtil.getEventType(event::class)]
                ?.invoke(this, event)
                ?.withCurrentEventNumber(eventNumber) as A?
                ?: this
