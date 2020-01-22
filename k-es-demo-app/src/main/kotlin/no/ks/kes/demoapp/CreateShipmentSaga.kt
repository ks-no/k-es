package no.ks.kes.demoapp

import no.ks.kes.lib.Saga
import java.util.*

data class CreateShipmentSagaState(val orderId: UUID)

class CreateShipmentSaga: Saga<CreateShipmentSagaState>(CreateShipmentSagaState::class) {

    init {
        initOn<BasketCheckedOut>({it.aggregateId}){
            val orderId = UUID.randomUUID()
            commands.add(ShipmentCmds.RequestShipment(orderId, it.items, it.aggregateId))
            CreateShipmentSagaState(orderId)
        }
    }

}