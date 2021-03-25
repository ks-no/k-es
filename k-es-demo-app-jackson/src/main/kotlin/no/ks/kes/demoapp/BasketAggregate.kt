package no.ks.kes.demoapp

import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.AggregateConfiguration
import no.ks.kes.lib.EventData
import no.ks.kes.lib.SerializationId
import java.util.*


data class BasketAggregate(
        val aggregateId: UUID,
        val basketContents: Map<UUID, Int> = emptyMap(),
        val basketClosed: Boolean = false
) : Aggregate

object Basket : AggregateConfiguration<BasketAggregate>("basket") {

    init {
        init { e: Created, aggregateId: UUID ->
            BasketAggregate(
                    aggregateId = aggregateId
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
    data class SessionStarted(val aggregateId: UUID) : EventData<BasketAggregate> {
        override fun upgrade(): EventData<BasketAggregate>? {
            return Created(aggregateId)
        }
    }

    @SerializationId("BasketCreated")
    data class Created(val aggregateId: UUID) : EventData<BasketAggregate>

    @SerializationId("BasketItemAdded")
    data class ItemAdded(val aggregateId: UUID, val itemId: UUID) : EventData<BasketAggregate>

    @SerializationId("BasketCheckedOut")
    data class CheckedOut(val aggregateId: UUID, val items: Map<UUID, Int>) : EventData<BasketAggregate>
}


