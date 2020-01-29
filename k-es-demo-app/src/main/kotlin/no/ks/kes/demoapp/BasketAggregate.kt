package no.ks.kes.demoapp

import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.Event
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.util.*


class Basket : Aggregate() {

    @SerializationId("BasketSessionStarted")
    @Deprecated("This event has been replaced by a newer version", replaceWith = ReplaceWith("Basket.Created(aggregateId, timestamp)"), level = DeprecationLevel.ERROR)
    data class SessionStarted(override val aggregateId: UUID, override val timestamp: Instant) : Event<Basket> {
        override fun upgrade(): Event<Basket>? {
            return Created(aggregateId, timestamp)
        }
    }

    @SerializationId("BasketCreated")
    data class Created(override val aggregateId: UUID, override val timestamp: Instant) : Event<Basket>

    @SerializationId("BasketItemAdded")
    data class ItemAdded(override val aggregateId: UUID, override val timestamp: Instant, val itemId: UUID) : Event<Basket>

    @SerializationId("BasketCheckedOut")
    data class CheckedOut(override val aggregateId: UUID, override val timestamp: Instant, val items: Map<UUID, Int>) : Event<Basket>

    override val aggregateType = "basket"
    var aggregateId: UUID? = null
    var basket: MutableMap<UUID, Int> = mutableMapOf()
    var basketClosed: Boolean = false

    init {
        on<Created> {
            aggregateId = it.aggregateId
        }

        on<ItemAdded> {
            basket.compute(it.itemId) { _, count -> count?.inc() ?: 1 }
        }

        on<CheckedOut> {
            basketClosed = true
        }
    }
}
