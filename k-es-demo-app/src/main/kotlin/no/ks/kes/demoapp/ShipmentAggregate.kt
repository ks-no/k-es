package no.ks.kes.demoapp

import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.AggregateConfiguration
import no.ks.kes.lib.Event
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.util.*

data class ShipmentAggregate(
        val aggregateId: UUID
): Aggregate

object Shipment : AggregateConfiguration<ShipmentAggregate>("shipment") {

    @SerializationId("ShipmentPrepared")
    data class Prepared(override val aggregateId: UUID, override val timestamp: Instant, val basketId: UUID, val items: Map<UUID, Int>) : Event<ShipmentAggregate>

    @SerializationId("ShipmentDelivered")
    data class Delivered(override val aggregateId: UUID, override val timestamp: Instant, val basketId: UUID) : Event<ShipmentAggregate>

    @SerializationId("ShipmentFailed")
    data class Failed(override val aggregateId: UUID, override val timestamp: Instant, val reason: String, val basketId: UUID) : Event<ShipmentAggregate>

    @SerializationId("WarehouseNotifiedOfMissingShipment")
    data class WarehouseNotifiedOfMissingShipment(override val aggregateId: UUID, override val timestamp: Instant, val basketId: UUID) : Event<ShipmentAggregate>

    init {
        init<Prepared> {
            ShipmentAggregate(aggregateId = it.aggregateId)
        }

        init<Failed> {
            ShipmentAggregate(aggregateId = it.aggregateId)
        }
    }
}
