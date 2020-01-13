package no.ks.kes.lib

import io.kotlintest.matchers.beInstanceOf
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import no.ks.kes.lib.testdomain.*
import java.time.Instant
import java.time.LocalDate
import java.util.*

class SagaTest : StringSpec() {
    init {
        "test that a saga can be initialized on an incoming event" {
            val cmdHandler = mockk<CmdHandler>().apply { every { handle(any<Cmd<*>>()) } returns Employee() }
            val event = HiredEvent(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now(),
                    timestamp = Instant.now()
            )
            SendHireNotificationSaga()
                    .apply {
                        setCmdHandler(cmdHandler)
                    }
                    .init(EventWrapper(
                            event,
                            0L))
                    .apply {
                        first shouldBe event.aggregateId.toString()
                        second shouldBe SagaState(addedToPayroll = false)
                    }
        }

        "test that a saga can derive a command from an incoming event" {
            val slot = slot<Cmd<*>>()
            val cmdHandler = mockk<CmdHandler>().apply { every { handle(capture(slot)) } returns Employee() }
            val event = HiredEvent(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now(),
                    timestamp = Instant.now()
            )
            SendHireNotificationSaga()
                    .apply {
                        setCmdHandler(cmdHandler)
                    }
                    .accept(EventWrapper(
                            event,
                            0L),
                            SagaState(addedToPayroll = false))

            slot.captured should beInstanceOf<AddToPayrollCmd>()
            slot.captured.aggregateId shouldBe event.aggregateId
        }

    }
}