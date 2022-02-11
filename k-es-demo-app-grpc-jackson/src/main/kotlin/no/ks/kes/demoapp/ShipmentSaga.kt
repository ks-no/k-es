package no.ks.kes.demoapp

import no.ks.kes.lib.Saga
import java.time.Instant
import java.util.*

data class ShipmentSagaState(
        val orderId: UUID,
        val basketId: UUID,
        val delivered: Boolean = false,
        val failed: Boolean = false
)

object ShipmentSaga : Saga<ShipmentSagaState>(ShipmentSagaState::class, "CreateShipmentSaga") {

    init {
        init { e: Basket.CheckedOut, aggregateId: UUID ->
            val shipmentId = UUID.randomUUID()
            dispatch(ShipmentCmds.Request(shipmentId, e.items, aggregateId))
            setState(ShipmentSagaState(shipmentId, aggregateId ))
        }

        apply({  e: Shipment.Delivered, aggregateId: UUID -> e.basketId }) { e: Shipment.Delivered, aggregateId: UUID ->
            setState(state.copy(delivered = true))
        }

        apply({ e: Shipment.Failed, aggregateId: UUID -> e.basketId }) { e: Shipment.Failed, aggregateId: UUID ->
            setState(state.copy(failed = true))
        }

        timeout({ e: Shipment.Prepared, aggregateId: UUID -> e.basketId }, { Instant.now().plusSeconds(5) }) {
            if (!state.delivered && !state.failed)
                dispatch(ShipmentCmds.SendMissingShipmentAlert(state.orderId, state.basketId))
        }
    }
}