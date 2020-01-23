package no.ks.kes.demoapp

import no.ks.kes.lib.AggregateRepository
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdHandler
import no.ks.kes.lib.CmdHandler.Result.*
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.util.*

class ShipmentCmds(repo: AggregateRepository, warehouseManager: WarehouseManager) : CmdHandler<Shipment>(repo) {
    override fun initAggregate(): Shipment = Shipment()

    @SerializationId("ShipmentRequest")
    data class Request(override val aggregateId: UUID, val items: Map<UUID, Int>, val basketId: UUID) : Cmd<Shipment>

    init {
        initOn<Request> {
            try {
                warehouseManager.shipOrder(it.aggregateId)
                Succeed(Shipment.Created(it.aggregateId, Instant.now(), it.basketId, it.items))
            } catch (e: ShipmentCreationException) {
                Fail(Shipment.Failed(it.aggregateId, Instant.now(), e.message!!, it.basketId), e)
            }
        }
    }
}

class ShipmentCreationException(message: String) : RuntimeException(message)

interface WarehouseManager {
    fun failNext()
    fun shipOrder(orderId: UUID)
}