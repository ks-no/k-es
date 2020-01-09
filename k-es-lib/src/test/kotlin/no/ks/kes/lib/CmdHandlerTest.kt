package no.ks.kes.lib

import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import no.ks.kes.lib.testdomain.Employee
import no.ks.kes.lib.testdomain.HireCmd
import java.time.LocalDate
import java.util.*

internal class CmdHandlerTest : StringSpec() {

    init {
        "Test that we can apply a cmd to a uninitialized aggregate, and that the derived state is returned" {
            val hireCmd = HireCmd(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now()
            )

            val readerMock = mockk<AggregateReader>().apply {
                every { read(hireCmd.aggregateId, ofType(Employee::class)) } returns Employee()
            }

            val writer = mockk<EventWriter>().apply {
                every { write("employee", hireCmd.aggregateId, 0, any(), true) } returns
                        Unit
            }
            CmdHandler(writer, readerMock)
                    .handle(hireCmd).apply {
                        aggregateId shouldBe hireCmd.aggregateId
                        startDate shouldBe hireCmd.startDate
                    }

        }

        "Test that the cmd handler throws an exception if invariants specified in the cmd are not met" {
            val hireCmd = HireCmd(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now()
            )

            val readerMock = mockk<AggregateReader>().apply {
                every { read(hireCmd.aggregateId, ofType(Employee::class)) } returns Employee().apply { aggregateId = UUID.randomUUID() }
            }

            shouldThrow<IllegalStateException> {
                CmdHandler(mockk(), readerMock)
                        .handle(hireCmd)
            }
                    .message shouldContain "The employee has already been created!"
        }

        "Test that the new aggregate state is applied to the event-writer"{
            val hireCmd = HireCmd(
                    aggregateId = UUID.randomUUID(),
                    startDate = LocalDate.now()
            )

            val readerMock = mockk<AggregateReader>().apply {
                every { read(hireCmd.aggregateId, ofType(Employee::class)) } returns Employee()
            }

            val writenEventsCapture = slot<List<Event<Employee>>>()
            val writerMock = mockk<EventWriter>().apply {
                every {
                    write(
                            aggregateType = "employee",
                            aggregateId = hireCmd.aggregateId,
                            expectedEventNumber = 0,
                            events = capture(writenEventsCapture),
                            useOptimisticLocking = true
                    )
                } returns Unit
            }

            CmdHandler(writerMock, readerMock)
                    .handle(hireCmd).apply {
                        aggregateId shouldBe hireCmd.aggregateId
                        startDate shouldBe hireCmd.startDate
                    }
        }
    }
}