package no.ks.kes.demoapp

import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.Event
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.util.*

@SerializationId("ShipmentCreated")
data class ShipmentCreated(override val aggregateId: UUID, override val timestamp: Instant) : Event<Shipment>

@SerializationId("CreateShipmentFailed")
data class CreateShipmentFailed(override val aggregateId: UUID, override val timestamp: Instant, val reason: String) : Event<Shipment>

class Shipment : Aggregate() {
    override val aggregateType = "order"
    var aggregateId: UUID? = null

    init {
        on<ShipmentCreated> {
            aggregateId = it.aggregateId
        }
    }
}
