package no.ks.kes.demoapp

import no.ks.kes.lib.Projection
import java.util.*

class Shipments : Projection() {
    private val created: MutableMap<UUID, Map<UUID, Int>> = mutableMapOf()
    private val failed: MutableSet<UUID> = mutableSetOf()

    init {
        on<Shipment.Created> { created.put(it.basketId, it.items) }
        on<Shipment.Failed> { failed.add(it.basketId) }
    }

    fun getShipments(basketId: UUID): Map<UUID, Int>? =
            created[basketId]

    fun isFailedShipment(basketId: UUID): Boolean = failed.contains(basketId)
}