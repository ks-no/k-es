package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Saga

class SendHireNotificationSaga : Saga<SendHireNotificationSaga.Payload>() {
    data class Payload(var addedToPayroll: Boolean)

    init {
        on<HiredEvent> {
            if (!addedToPayroll)
                dispatch(AddToPayrollCmd(it.aggregateId))
        }

        on<AddedToPayroll> {
            addedToPayroll = true
        }
    }
}