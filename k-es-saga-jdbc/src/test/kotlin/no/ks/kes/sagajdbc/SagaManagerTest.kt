package no.ks.kes.sagajdbc

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import no.ks.kes.sagajdbc.testdomain.ConfidentialityAgreementAccepted
import no.ks.kes.sagajdbc.testdomain.HiredEvent
import java.time.Instant
import java.time.LocalDate
import java.util.*
import kotlin.reflect.KClass

class SagaManagerTest: StringSpec() {

    init {
        "test that an event with an initializer with conforming correlation id is initialized when the event arrives" {
            data class SomeSagaState(val someId: UUID)

            @SagaName("foo")
            class SomeSaga: Saga<SomeSagaState>(SomeSagaState::class){
                init {
                    initOn<HiredEvent>({aggregateId}) { SomeSagaState(it.aggregateId) }
                }
            }

            val event = HiredEvent(
                    aggregateId = UUID.randomUUID(),
                    recruitedBy = UUID.randomUUID(),
                    startDate = LocalDate.now(),
                    timestamp = Instant.now()
            )

            val subSlot = slot<(EventWrapper<*>) -> Unit>()
            val sagaStateSlot = slot<SomeSagaState>()
            val eventSubscriber = mockk<EventSubscriber>().apply { every { onEvent(capture(subSlot)) } returns Unit}
            val sagaRepository = mockk<SagaRepository>().apply {
                every { save(eq(event.aggregateId), "foo", eq(ByteArray(10))) } returns Unit
            }
            val sagaSerdes = mockk<SagaSerdes>().apply {
                every { serialize(capture(sagaStateSlot)) } returns ByteArray(10)
            }

            SagaManager(eventSubscriber, sagaRepository, sagaSerdes, setOf(SomeSaga()))
            subSlot.captured.invoke(EventWrapper(event, 0L))

            sagaStateSlot.captured.someId shouldBe event.aggregateId
        }

        "test that a handler in an initialized saga is invoked when the specified event with a conforming correlation-id arrives" {
            data class SomeSagaState(val someId: UUID, val accepted: Boolean = false)

            @SagaName("foo")
            class SomeSaga: Saga<SomeSagaState>(SomeSagaState::class){
                init {
                    initOn<HiredEvent>({aggregateId}) { SomeSagaState(it.aggregateId) }
                    on<ConfidentialityAgreementAccepted>({aggregateId}) { copy(accepted  = true)}
                }
            }

            val event = ConfidentialityAgreementAccepted(
                    aggregateId = UUID.randomUUID(),
                    timestamp = Instant.now()
            )

            val subSlot = slot<(EventWrapper<*>) -> Unit>()
            val sagaStateSlot = slot<SomeSagaState>()
            val eventSubscriber = mockk<EventSubscriber>().apply { every { onEvent(capture(subSlot)) } returns Unit}
            val sagaRepository = mockk<SagaRepository>().apply {
                every { save(eq(event.aggregateId), "foo", eq(ByteArray(10))) } returns Unit
                every { get(eq(event.aggregateId), "foo") } returns ByteArray(10)
            }
            val sagaSerdes = mockk<SagaSerdes>().apply {
                every { deserialize(eq(ByteArray(10)), eq(SomeSagaState::class as KClass<Any>)) } returns SomeSagaState(event.aggregateId)
                every { serialize(capture(sagaStateSlot)) } returns ByteArray(10)
            }

            SagaManager(eventSubscriber, sagaRepository, sagaSerdes, setOf(SomeSaga()))
            subSlot.captured.invoke(EventWrapper(event, 0L))

            sagaStateSlot.captured.someId shouldBe event.aggregateId
            sagaStateSlot.captured.accepted shouldBe true
        }
    }
}