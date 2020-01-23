package no.ks.kes.demoapp

import no.ks.kes.lib.Saga
import no.ks.kes.lib.SerializationId
import java.util.*

data class CreateShipmentSagaState(val orderId: UUID)

@SerializationId("CreateShipmentSaga")
class CreateShipmentSaga : Saga<CreateShipmentSagaState>(CreateShipmentSagaState::class) {

    init {
        initOn<Basket.CheckedOut>({ it.aggregateId }) {
            val orderId = UUID.randomUUID()
            commands.add(ShipmentCmds.Request(orderId, it.items, it.aggregateId))
            CreateShipmentSagaState(orderId)
        }
    }

}