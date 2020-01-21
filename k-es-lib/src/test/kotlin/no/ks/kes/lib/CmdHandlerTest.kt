package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import no.ks.kes.lib.testdomain.Employee
import no.ks.kes.lib.testdomain.HiredEvent
import java.time.Instant
import java.time.LocalDate
import java.util.*

internal class CmdHandlerTest : StringSpec() {

    init {
        "Test that a cmd can initialize an aggregate, and that the derived state is returned" {
            data class HireCmd(override val aggregateId: UUID, val startDate: LocalDate) : Cmd<Employee>

            val hireCmd = HireCmd(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now()
            )

            val readerMock = mockk<AggregateReader>().apply {
                every { read(hireCmd.aggregateId, ofType(Employee::class)) } returns null
            }

            val writer = mockk<EventWriter>().apply {
                every { write("employee", hireCmd.aggregateId, 0, any(), true) } returns
                        Unit
            }

            class EmployeeCmdHandler() : CmdHandler<Employee>(writer, readerMock) {
                override fun initAggregate(): Employee = Employee()

                init {
                    initOn<HireCmd> {
                        Result.Succeed(HiredEvent(it.aggregateId, UUID.randomUUID(), LocalDate.now(), Instant.now()))
                    }
                }
            }

            EmployeeCmdHandler().handle(hireCmd).apply { aggregateId shouldBe hireCmd.aggregateId }
        }
    }
}
