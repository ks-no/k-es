package no.ks.kes.demoapp

import no.ks.kes.lib.Projection
import java.util.*

class MostPopularItem: Projection() {
    private val itemOrders: MutableMap<UUID, Int> = mutableMapOf()

    init {
        on<ShipmentCreated> { itemOrders.getOrDefault(it.aggregateId, 0).inc() }
    }

    fun getMostPopularItem(): UUID? =
            itemOrders.entries.maxBy { it.value }?.key
}