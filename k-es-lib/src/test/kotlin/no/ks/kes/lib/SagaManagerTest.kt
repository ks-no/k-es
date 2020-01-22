package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import no.ks.kes.lib.testdomain.ConfidentialityAgreementAccepted
import no.ks.kes.lib.testdomain.Hired
import java.time.Instant
import java.time.LocalDate
import java.util.*

class SagaManagerTest: StringSpec() {

    init {
        "test that an event with an initializer with conforming correlation id is initialized when the event arrives" {
            data class SomeSagaState(val someId: UUID)

            @SerializationId("foo")
            class SomeSaga: Saga<SomeSagaState>(SomeSagaState::class){
                init {
                    initOn<Hired>({it.aggregateId}) { SomeSagaState(it.aggregateId) }
                }
            }

            val event = Hired(
                    aggregateId = UUID.randomUUID(),
                    recruitedBy = UUID.randomUUID(),
                    startDate = LocalDate.now(),
                    timestamp = Instant.now()
            )

            val subSlot = slot<(EventWrapper<*>) -> Unit>()
            val sagaStateSlot = slot<Set<SagaRepository.SagaUpsert.SagaInsert>>()
            val eventSubscriber = mockk<EventSubscriber>().apply { every { subscribe(capture(subSlot)) } returns Unit}
            val sagaRepository = mockk<SagaRepository>().apply {
                every { update(any(), capture(sagaStateSlot)) } returns Unit
                every { getCurrentHwm() } returns 0L
            }

            SagaManager(eventSubscriber, sagaRepository, setOf(SomeSaga()))
            subSlot.captured.invoke(EventWrapper(event, 0L))

            sagaStateSlot.captured.single().newState shouldBe SomeSagaState(event.aggregateId)
        }

        "test that a handler in an initialized saga is invoked when the specified event with a conforming correlation-id arrives" {
            data class SomeSagaState(val someId: UUID, val accepted: Boolean = false)

            @SerializationId("foo")
            class SomeSaga: Saga<SomeSagaState>(SomeSagaState::class){
                init {
                    initOn<Hired>({it.aggregateId}) { SomeSagaState(it.aggregateId) }
                    on<ConfidentialityAgreementAccepted>({it.aggregateId}) { state.copy(accepted  = true)}
                }
            }

            val event = ConfidentialityAgreementAccepted(
                    aggregateId = UUID.randomUUID(),
                    timestamp = Instant.now()
            )

            val subSlot = slot<(EventWrapper<*>) -> Unit>()
            val sagaStateSlot = slot<Set<SagaRepository.SagaUpsert.SagaUpdate>>()
            val eventSubscriber = mockk<EventSubscriber>().apply { every { subscribe(capture(subSlot)) } returns Unit}
            val sagaRepository = mockk<SagaRepository>().apply {
                every { update(any(), capture(sagaStateSlot)) } returns Unit
                every { getCurrentHwm() } returns 0L
                every { getSagaState(eq(event.aggregateId), "foo", SomeSagaState::class) } returns SomeSagaState(event.aggregateId, false)
            }


            SagaManager(eventSubscriber, sagaRepository, setOf(SomeSaga()))
            subSlot.captured.invoke(EventWrapper(event, 0L))

            sagaStateSlot.captured.single().newState shouldBe SomeSagaState(event.aggregateId, true)
        }
    }
}