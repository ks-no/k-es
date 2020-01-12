package no.ks.kes.sagalib.testdomain

import no.ks.kes.lib.testdomain.AddToPayrollCmd
import no.ks.kes.lib.testdomain.AddedToPayroll
import no.ks.kes.lib.testdomain.HiredEvent
import no.ks.kes.sagalib.Saga


class SendHireNotificationSaga : Saga<SagaState>()
 {
    init {
        createOn<HiredEvent> {
            it.aggregateId.toString() to SagaState(false)
        }

        on<HiredEvent> {
            if (!addedToPayroll)
                dispatch(AddToPayrollCmd(it.aggregateId))
        }

        on<AddedToPayroll> {
            addedToPayroll = true
        }
    }
}

data class SagaState(var addedToPayroll: Boolean)