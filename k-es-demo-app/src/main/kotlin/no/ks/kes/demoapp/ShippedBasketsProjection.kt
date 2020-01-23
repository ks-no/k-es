package no.ks.kes.demoapp

import no.ks.kes.lib.Projection
import java.util.*

class ShippedBaskets: Projection() {
    private val itemOrders: MutableMap<UUID, Map<UUID, Int>> = mutableMapOf()

    init {
        on<Shipment.Created> { itemOrders.put(it.basketId, it.items)}
    }

    fun getShippedBasket(basketId: UUID): Map<UUID, Int>? =
            itemOrders[basketId]
}