package no.ks.kes.demoapp

import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.AggregateConfiguration
import no.ks.kes.lib.Event
import no.ks.kes.serdes.jackson.SerializationId
import java.util.*


data class BasketAggregate(
        val aggregateId: UUID,
        val basketContents: Map<UUID, Int> = emptyMap(),
        val basketClosed: Boolean = false
) : Aggregate

object Basket : AggregateConfiguration<BasketAggregate>("basket") {

    init {
        init<Created> {
            BasketAggregate(
                    aggregateId = it.aggregateId
            )
        }

        apply<ItemAdded> {
            copy(
                    basketContents = basketContents + (it.itemId to basketContents.getOrDefault(it.itemId, 0).inc())
            )
        }

        apply<CheckedOut> {
            copy(
                    basketClosed = true
            )
        }
    }

    @SerializationId("BasketSessionStarted")
    @Deprecated("This event has been replaced by a newer version", replaceWith = ReplaceWith("Basket.Created(aggregateId, timestamp)"), level = DeprecationLevel.ERROR)
    data class SessionStarted(override val aggregateId: UUID) : Event<BasketAggregate> {
        override fun upgrade(): Event<BasketAggregate>? {
            return Created(aggregateId)
        }
    }

    @SerializationId("BasketCreated")
    data class Created(override val aggregateId: UUID) : Event<BasketAggregate>

    @SerializationId("BasketItemAdded")
    data class ItemAdded(override val aggregateId: UUID, val itemId: UUID) : Event<BasketAggregate>

    @SerializationId("BasketCheckedOut")
    data class CheckedOut(override val aggregateId: UUID, val items: Map<UUID, Int>) : Event<BasketAggregate>
}


