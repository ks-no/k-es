package no.ks.kes.demoapp

import no.ks.kes.lib.Saga
import java.util.*

data class ReceiptSagaState(val orderId: UUID)

class CreateOrderSaga: Saga<ReceiptSagaState>(ReceiptSagaState::class) {

    init {
        initOn<BasketCheckedOut>({it.aggregateId}){
            val orderId = UUID.randomUUID()
            commands.add(ShipmentCmds.RequestShipment(orderId, it.items))
            ReceiptSagaState(orderId)
        }
    }

}