package no.ks.kes.lib.testdomain

import no.ks.kes.lib.Saga


class SendHireNotificationSaga : Saga<SagaState>()
{
    init {
        initOn<HiredEvent> {
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