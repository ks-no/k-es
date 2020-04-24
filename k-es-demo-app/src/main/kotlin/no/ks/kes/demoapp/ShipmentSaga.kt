package no.ks.kes.demoapp

import no.ks.kes.lib.Saga
import java.util.*

data class ShipmentSagaState(val orderId: UUID, val basketId: UUID, val delivered: Boolean = false, val failed: Boolean = false)

object ShipmentSaga : Saga<ShipmentSagaState>(ShipmentSagaState::class, "CreateShipmentSaga") {

    init {
        init<Basket.CheckedOut> {
            val shipmentId = UUID.randomUUID()
            dispatch(ShipmentCmds.Request(shipmentId, it.items, it.aggregateId))
            setState(ShipmentSagaState(shipmentId, it.aggregateId))
        }

        apply<Shipment.Delivered>({ it.basketId }) {
            setState(state.copy(delivered = true))
        }

        apply<Shipment.Failed>({ it.basketId }) {
            setState(state.copy(failed = true))
        }

        timeout<Shipment.Prepared>({ it.basketId }, { it.timestamp.plusSeconds(5) }) {
            if (!state.delivered && !state.failed)
                dispatch(ShipmentCmds.SendMissingShipmentAlert(state.orderId, state.basketId))
        }
    }
}