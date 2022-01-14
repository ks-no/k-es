package no.ks.kes.demoapp

import no.ks.kes.lib.Projection
import java.util.*

class Shipments : Projection() {
    private val created: MutableMap<UUID, Map<UUID, Int>> = mutableMapOf()
    private val failed: MutableSet<UUID> = mutableSetOf()
    private val missing: MutableSet<UUID> = mutableSetOf()

    init {
        on<Shipment.Prepared> { created.put(it.eventData.basketId, it.eventData.items) }
        on<Shipment.Failed> { failed.add(it.eventData.basketId) }
        on<Shipment.WarehouseNotifiedOfMissingShipment> { missing.add(it.eventData.basketId) }
    }

    @Synchronized
    fun getShipments(basketId: UUID): Map<UUID, Int>? =
            created[basketId]

    @Synchronized
    fun isFailedShipment(basketId: UUID): Boolean = failed.contains(basketId)

    @Synchronized
    fun isMissingShipment(basketId: UUID): Boolean = missing.contains(basketId)
}