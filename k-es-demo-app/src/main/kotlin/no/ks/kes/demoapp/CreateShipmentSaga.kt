package no.ks.kes.demoapp

import no.ks.kes.lib.Saga
import no.ks.kes.lib.SerializationId
import java.util.*

data class CreateShipmentSagaState(val orderId: UUID, val basketId: UUID, val delivered: Boolean = false,  val failed: Boolean = false)

@SerializationId("CreateShipmentSaga")
class CreateShipmentSaga : Saga<CreateShipmentSagaState>(CreateShipmentSagaState::class) {

    init {
        initOn<Basket.CheckedOut> {
            val shipmentId = UUID.randomUUID()
            dispatch(ShipmentCmds.Request(shipmentId, it.items, it.aggregateId))
            setState(CreateShipmentSagaState(shipmentId, it.aggregateId))
        }

        on<Shipment.Delivered>({it.basketId}){
            setState(state.copy(delivered = true))
        }

        on<Shipment.Failed>({it.basketId}){
            setState(state.copy(failed = true))
        }

        createTimeoutOn<Shipment.Created>({it.basketId}, {it.timestamp.plusSeconds(5)}) {
            if (!state.delivered && !state.failed)
                dispatch(ShipmentCmds.SendMissingShipmentAlert(state.orderId, state.basketId))
        }
    }
}