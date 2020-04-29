package no.ks.kes.demoapp

import no.ks.kes.lib.AggregateRepository
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdHandler
import no.ks.kes.lib.CmdHandler.Result.*
import no.ks.kes.serdes.jackson.SerializationId
import java.time.Instant
import java.util.*

class ShipmentCmds(repo: AggregateRepository, warehouseManager: WarehouseManager) : CmdHandler<ShipmentAggregate>(repo, Shipment) {

    init {
        init<Request> {
            try {
                warehouseManager.shipOrder(it.aggregateId)
                Succeed(Shipment.Prepared(it.aggregateId, it.basketId, it.items))
            } catch (e: ItemNoLongerCarried) {
                Fail(Shipment.Failed(it.aggregateId, "Item no longer carried!", it.basketId), e)
            } catch (e: WarehouseSystemFailure) {
                RetryOrFail(Shipment.Failed(it.aggregateId,  "System problem!", it.basketId), e) { Instant.now() }
            }
        }

        apply<SendMissingShipmentAlert> {
            warehouseManager.investigateMissingShipment(it.aggregateId)
            Succeed(Shipment.WarehouseNotifiedOfMissingShipment(it.aggregateId, it.basketId))
        }
    }

    @SerializationId("ShipmentRequest")
    data class Request(override val aggregateId: UUID, val items: Map<UUID, Int>, val basketId: UUID) : Cmd<ShipmentAggregate>

    @SerializationId("SendMissingShipmentAlert")
    data class SendMissingShipmentAlert(override val aggregateId: UUID, val basketId: UUID) : Cmd<ShipmentAggregate>
}

class ItemNoLongerCarried : RuntimeException()
class WarehouseSystemFailure : RuntimeException()

interface WarehouseManager {
    fun failOnce(e: Exception?)
    fun investigateMissingShipment(orderId: UUID)
    fun shipOrder(orderId: UUID)
}