package no.ks.kes.lib

import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import no.ks.kes.lib.testdomain.Employee
import no.ks.kes.lib.testdomain.Hired
import no.ks.kes.lib.testdomain.StartDateChanged
import java.time.Instant
import java.time.LocalDate
import java.util.*

internal class AsyncCmdHandlerTest : StringSpec() {

    init {
        "Test that a cmd can initialize an aggregate" {
            data class HireCmd(override val aggregateId: UUID, val startDate: LocalDate, val recruitedBy: UUID) : Cmd<Employee>

            val hireCmd = HireCmd(
                    aggregateId = UUID.randomUUID(),
                    recruitedBy = UUID.randomUUID(),
                    startDate = LocalDate.now()
            )

            val readerMock = mockk<AggregateReader>().apply {
                every { read(hireCmd.aggregateId, ofType(Employee::class)) } returns Employee().withCurrentEventNumber(-1)
            }

            val slot = slot<List<Event<*>>>()

            val writer = mockk<AggregateRepository>().apply {
                every { write("employee", hireCmd.aggregateId, ExpectedEventNumber.AggregateDoesNotExist, capture(slot)) } returns
                        Unit
            }

            class EmployeeCmdHandler() : CmdHandler<Employee>(writer, readerMock) {
                override fun initAggregate(): Employee = Employee()

                init {
                    initOn<HireCmd> {
                        Result.Succeed(Hired(it.aggregateId, it.recruitedBy, it.startDate, Instant.now()))
                    }
                }
            }

            EmployeeCmdHandler().handleAsync(hireCmd, 0).apply { this.shouldBeInstanceOf<CmdHandler.AsyncResult.Success>() }
            with(slot.captured.single() as Hired) {
                aggregateId shouldBe hireCmd.aggregateId
                recruitedBy shouldBe hireCmd.recruitedBy
                startDate shouldBe hireCmd.startDate
            }
        }

        "Test that a cmd can append a new event to an existing aggregate, and that the derived state is returned" {
            data class ChangeStartDate(override val aggregateId: UUID, val newStartDate: LocalDate) : Cmd<Employee>

            val changeStartDate = ChangeStartDate(
                    aggregateId = UUID.randomUUID(),
                    newStartDate = LocalDate.now()
            )

            val readerMock = mockk<AggregateReader>().apply {
                every { read(changeStartDate.aggregateId, ofType(Employee::class)) } returns Employee()
                        .applyEvent(Hired(changeStartDate.aggregateId, UUID.randomUUID(), LocalDate.now(), Instant.now()), 0)
            }
            val slot = slot<List<Event<*>>>()

            val writer = mockk<AggregateRepository>().apply {
                every { write("employee", changeStartDate.aggregateId, ExpectedEventNumber.Exact(0), capture(slot)) } returns
                        Unit
            }

            class EmployeeCmdHandler() : CmdHandler<Employee>(writer, readerMock) {
                override fun initAggregate(): Employee = Employee()

                init {
                    on<ChangeStartDate> {
                        Result.Succeed(StartDateChanged(it.aggregateId, it.newStartDate, Instant.now()))
                    }
                }
            }

            EmployeeCmdHandler().handleAsync(changeStartDate, 0).apply { this.shouldBeInstanceOf<CmdHandler.AsyncResult.Success>() }
            with(slot.captured.single() as StartDateChanged) {
                aggregateId shouldBe changeStartDate.aggregateId
                newStartDate shouldBe changeStartDate.newStartDate
            }
        }

    }
}
