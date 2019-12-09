package no.ks.kes.lib

abstract class Aggregate<EVENT_TYPE : Event> {
    var applicators: MutableMap<String, (Aggregate<EVENT_TYPE>, EVENT_TYPE) -> Aggregate<EVENT_TYPE>> = mutableMapOf()
        private set
    var currentEventNumber: Long = 0
        internal set

    inline fun <reified E : Event> on(crossinline consumer: (E) -> Unit) {
        applicators[EventUtil.getEventType(E::class)] =
                { a: Aggregate<EVENT_TYPE>, e: EVENT_TYPE -> consumer(EventUpgrader.upgradeTo(e, E::class)); a }
    }

    abstract val aggregateType: String
}

fun <EVENT_TYPE : Event, T : Aggregate<EVENT_TYPE>> T.withCurrentEventNumber(currentEventNumber: Long): T =
        apply { this.currentEventNumber = currentEventNumber }

fun <EVENT_TYPE : Event, T : Aggregate<EVENT_TYPE>> T.applyEvent(event: EVENT_TYPE, eventNumber: Long): T =
        applicators[EventUtil.getEventType(event::class)]
                ?.invoke(this, event)
                ?.withCurrentEventNumber(eventNumber) as T?
                ?: this
