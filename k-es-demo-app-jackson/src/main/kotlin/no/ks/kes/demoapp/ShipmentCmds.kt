package no.ks.kes.demoapp

import no.ks.kes.lib.*
import no.ks.kes.lib.CmdHandler.Result.*
import java.time.Instant
import java.util.*

class ShipmentCmds(repo: AggregateRepository, warehouseManager: WarehouseManager) : CmdHandler<ShipmentAggregate>(repo, Shipment) {

    init {
        init<Request> {
            try {
                warehouseManager.shipOrder(it.aggregateId)
                Succeed(
                    Event( eventData = Shipment.Prepared(it.aggregateId, it.basketId, it.items), aggregateId = it.aggregateId))
            } catch (e: ItemNoLongerCarried) {
                Fail(
                    Event( eventData = Shipment.Failed(it.aggregateId, "Item no longer carried!", it.basketId), aggregateId = it.aggregateId), e)
            } catch (e: WarehouseSystemFailure) {
                RetryOrFail(
                    Event(  eventData = Shipment.Failed(it.aggregateId,  "System problem!", it.basketId), aggregateId = it.aggregateId), e) { Instant.now() }
            }
        }

        apply<SendMissingShipmentAlert> {
            warehouseManager.investigateMissingShipment(it.aggregateId)
            Succeed(
                Event( eventData = Shipment.WarehouseNotifiedOfMissingShipment(it.aggregateId, it.basketId), aggregateId = it.aggregateId))
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