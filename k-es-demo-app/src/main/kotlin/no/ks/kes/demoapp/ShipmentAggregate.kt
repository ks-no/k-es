package no.ks.kes.demoapp

import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.Event
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.util.*


class Shipment : Aggregate() {
    @SerializationId("ShipmentCreated")
    data class Created(override val aggregateId: UUID, override val timestamp: Instant, val basketId: UUID, val items: Map<UUID, Int>) : Event<Shipment>

    @SerializationId("ShipmentDelivered")
    data class Delivered(override val aggregateId: UUID, override val timestamp: Instant) : Event<Shipment>

    @SerializationId("ShipmentFailed")
    data class Failed(override val aggregateId: UUID, override val timestamp: Instant, val reason: String, val basketId: UUID) : Event<Shipment>

    override val aggregateType = "shipment"
    var aggregateId: UUID? = null

    init {
        on<Created> {
            aggregateId = it.aggregateId
        }

        on<Failed> {
            aggregateId = it.aggregateId
        }
    }
}
