package no.ks.kes.demoapp

import no.ks.kes.lib.Saga
import no.ks.kes.lib.SerializationId
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.DAYS
import java.util.*

data class CreateShipmentSagaState(val orderId: UUID, val delivered: Boolean = false)

@SerializationId("CreateShipmentSaga")
class CreateShipmentSaga : Saga<CreateShipmentSagaState>(CreateShipmentSagaState::class) {

    init {
        initOn<Basket.CheckedOut> {
            val orderId = UUID.randomUUID()
            dispatch(ShipmentCmds.Request(orderId, it.items, it.aggregateId))
            setState(CreateShipmentSagaState(orderId))
        }

        on<Shipment.Delivered> {
            setState(state.copy(delivered = true))
        }

        createTimeoutOn<Shipment.Created>({it.timestamp.plus(2, DAYS)}) {
            if (!state.delivered)
                dispatch(ShipmentCmds.SendLateShipmentAlert(state.orderId))
        }
    }
}