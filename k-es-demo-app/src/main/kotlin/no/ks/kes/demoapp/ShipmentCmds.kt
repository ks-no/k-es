package no.ks.kes.demoapp

import no.ks.kes.lib.AggregateRepository
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdHandler
import java.time.Instant
import java.util.*

class ShipmentCmds(repo: AggregateRepository, warehouseManager: WarehouseManager) : CmdHandler<Shipment>(repo) {
    override fun initAggregate(): Shipment = Shipment()

    data class RequestShipment(override val aggregateId: UUID, val items: Map<UUID, Int>, val basketId: UUID) : Cmd<Shipment>

    init {
        initOn<RequestShipment> {
            try {
                warehouseManager.shipOrder(it.aggregateId)
                Result.Succeed(ShipmentCreated(it.aggregateId, Instant.now(), it.basketId, it.items))
            } catch (e: ShipmentCreationException){
                Result.RetryOrFail(CreateShipmentFailed(it.aggregateId, Instant.now(), e.message!!), e)
            }
        }
    }
}

class ShipmentCreationException: RuntimeException()

interface WarehouseManager {
    fun shipOrder(orderId: UUID)
}
