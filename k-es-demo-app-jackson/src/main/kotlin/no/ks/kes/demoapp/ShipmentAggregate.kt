package no.ks.kes.demoapp

import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.AggregateConfiguration
import no.ks.kes.lib.Event
import no.ks.kes.lib.SerializationId
import java.util.*

data class ShipmentAggregate(
        val aggregateId: UUID
) : Aggregate

object Shipment : AggregateConfiguration<ShipmentAggregate>("shipment") {

    @SerializationId("ShipmentPrepared")
    data class Prepared(val aggregateId: UUID, val basketId: UUID, val items: Map<UUID, Int>) : Event<ShipmentAggregate>

    @SerializationId("ShipmentDelivered")
    data class Delivered(val aggregateId: UUID, val basketId: UUID) : Event<ShipmentAggregate>

    @SerializationId("ShipmentFailed")
    data class Failed(val aggregateId: UUID, val reason: String, val basketId: UUID) : Event<ShipmentAggregate>

    @SerializationId("WarehouseNotifiedOfMissingShipment")
    data class WarehouseNotifiedOfMissingShipment(val aggregateId: UUID, val basketId: UUID) : Event<ShipmentAggregate>

    init {
        init { e: Prepared, aggregateId: UUID ->
            ShipmentAggregate(aggregateId = aggregateId)
        }

        init { e: Failed, aggregateId: UUID ->
            ShipmentAggregate(aggregateId = aggregateId)
        }
    }
}
