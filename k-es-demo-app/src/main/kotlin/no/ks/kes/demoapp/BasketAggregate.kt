package no.ks.kes.demoapp

import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.Event
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.util.*

@SerializationId("SessionStarted")
data class SessionStarted(override val aggregateId: UUID, override val timestamp: Instant) : Event<Basket>

@SerializationId("ItemAddedToBasket")
data class ItemAddedToBasket(override val aggregateId: UUID, override val timestamp: Instant, val itemId: UUID) : Event<Basket>

@SerializationId("BasketCheckedOut")
data class BasketCheckedOut(override val aggregateId: UUID, override val timestamp: Instant, val items: Map<UUID, Int>) : Event<Basket>

class Basket() : Aggregate() {
    override val aggregateType = "basket"
    var aggregateId: UUID? = null
    var basket: MutableMap<UUID, Int> = mutableMapOf()
    var basketClosed: Boolean = false

    init {
        on<SessionStarted> {
            aggregateId = it.aggregateId
        }

        on<ItemAddedToBasket> {
            basket.getOrPut(it.itemId, { 0 }).inc()
        }

        on<BasketCheckedOut> {
            basketClosed = true
        }

    }
}
