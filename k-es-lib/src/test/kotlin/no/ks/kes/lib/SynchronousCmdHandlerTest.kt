package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import no.ks.kes.lib.testdomain.Employee
import no.ks.kes.lib.testdomain.HiredEvent
import no.ks.kes.lib.testdomain.StartDateChanged
import java.lang.IllegalStateException
import java.time.Instant
import java.time.LocalDate
import java.util.*

internal class SynchronousCmdHandlerTest : StringSpec() {

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

        "Test that a command can result in an event being applied to an existing aggregate" {
            data class ChangeStartDate(override val aggregateId: UUID, val newStartDate: LocalDate) : Cmd<Employee>

            val changeStartDate = ChangeStartDate(
                    aggregateId = UUID.randomUUID(),
                    newStartDate = LocalDate.now()
            )

            val readerMock = mockk<AggregateReader>().apply {
                every { read(changeStartDate.aggregateId, ofType(Employee::class)) } returns Employee()
                        .applyEvent(HiredEvent(changeStartDate.aggregateId, UUID.randomUUID(), LocalDate.now(), Instant.now()), 0)
            }

            val writer = mockk<EventWriter>().apply {
                every { write("employee", changeStartDate.aggregateId, 0, any(), true) } returns
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

            EmployeeCmdHandler().handle(changeStartDate).apply {
                aggregateId shouldBe changeStartDate.aggregateId
                startDate shouldBe changeStartDate.newStartDate
            }
        }

        "Test that a command failure results in an exception being thrown" {
            data class ChangeStartDate(override val aggregateId: UUID, val newStartDate: LocalDate) : Cmd<Employee>

            val changeStartDate = ChangeStartDate(
                    aggregateId = UUID.randomUUID(),
                    newStartDate = LocalDate.now()
            )

            val readerMock = mockk<AggregateReader>().apply {
                every { read(changeStartDate.aggregateId, ofType(Employee::class)) } returns Employee()
                        .applyEvent(HiredEvent(changeStartDate.aggregateId, UUID.randomUUID(), LocalDate.now(), Instant.now()), 0)
            }

            val writer = mockk<EventWriter>().apply {
                every { write("employee", changeStartDate.aggregateId, 0, any(), true) } returns
                        Unit
            }

            class EmployeeCmdHandler() : CmdHandler<Employee>(writer, readerMock) {
                override fun initAggregate(): Employee = Employee()

                init {
                    on<ChangeStartDate> {
                        Result.Fail(IllegalStateException("some invalid state"))
                    }
                }
            }

            shouldThrow<IllegalStateException> { EmployeeCmdHandler().handle(changeStartDate) }
                    .message shouldBe "some invalid state"
        }
    }
}
