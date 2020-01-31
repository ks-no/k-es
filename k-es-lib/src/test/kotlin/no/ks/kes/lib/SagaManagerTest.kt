package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.ks.kes.lib.testdomain.Hired
import no.ks.kes.lib.testdomain.StartDateChanged
import java.time.Instant
import java.time.LocalDate
import java.util.*

class SagaManagerTest : StringSpec() {
    init {
        "test that the saga manager should extract a Insert with a new state when the saga is initialized" {
            data class SomeState(val id: UUID)

            val saga = @SerializationId("SomeState") object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<Hired>({ it.aggregateId }) {
                        setState(SomeState(it.aggregateId))
                    }
                }
            }

            val sagaManager = Sagas(setOf(saga)) { _, _ -> null }

            val event = Hired(UUID.randomUUID(), UUID.randomUUID(), LocalDate.now(), Instant.now())
            val onEvent = sagaManager.onEvent(EventWrapper(event, 0L))

            with(onEvent.single() as SagaRepository.SagaUpsert.SagaInsert) {
                newState shouldBe SomeState(event.aggregateId)
            }
        }

        "test that the saga manager should extract a Update with a new state when onEvent is invoked after initialization" {
            data class SomeState(val id: UUID, val startDate: LocalDate)

            val event = StartDateChanged(UUID.randomUUID(), LocalDate.now(), Instant.now())
            val onEvent = Sagas(
                    setOf<Saga<SomeState>>(@SerializationId("SomeState") object : Saga<SomeState>(SomeState::class) {
                        init {
                            initOn<Hired> {
                                setState(SomeState(it.aggregateId, it.startDate))
                            }

                            on<StartDateChanged> {
                                setState(state.copy(startDate = it.newStartDate))
                            }
                        }
                    })) { _, _ -> SomeState(event.aggregateId, LocalDate.now().minusDays(1)) }
                    .onEvent(EventWrapper(event, 0L))

            with(onEvent.single() as SagaRepository.SagaUpsert.SagaUpdate) {
                newState shouldBe SomeState(event.aggregateId, event.newStartDate)
            }
        }

        "test that the saga manager should extract \"create timeout\" when one is configured as an event reaction" {
            data class SomeState(val id: UUID, val startDate: LocalDate)

            val event = StartDateChanged(UUID.randomUUID(), LocalDate.now(), Instant.now())

            val timeoutTime = Instant.now()
            val onEvent = Sagas(
                    setOf<Saga<SomeState>>(@SerializationId("SomeState") object : Saga<SomeState>(SomeState::class) {
                        init {
                            initOn<Hired> {
                                setState(SomeState(it.aggregateId, it.startDate))
                            }

                            createTimeoutOn<StartDateChanged>({it.aggregateId}, {timeoutTime}) {}
                        }
                    })) { _, _ -> SomeState(event.aggregateId, LocalDate.now().minusDays(1))}
                    .onEvent(EventWrapper(event, 0L))

            with(onEvent.single() as SagaRepository.SagaUpsert.SagaUpdate) {
                timeouts.single() shouldBe Saga.Timeout(timeoutTime, AnnotationUtil.getSerializationId(StartDateChanged::class))
            }
        }

        "test that the saga manager should execute the timeout handler when a triggered timeout arrives" {
            data class SomeState(val id: UUID, val startDate: LocalDate, val timeoutTriggered: Boolean = false)

            val event = StartDateChanged(UUID.randomUUID(), LocalDate.now(), Instant.now())

            val timeoutTime = Instant.now()
            val result = Sagas(
                    setOf<Saga<SomeState>>(@SerializationId("SomeState") object : Saga<SomeState>(SomeState::class) {
                        init {
                            initOn<Hired> {
                                setState(SomeState(it.aggregateId, it.startDate))
                            }

                            createTimeoutOn<StartDateChanged>({it.aggregateId}, {timeoutTime}) {setState(state.copy(timeoutTriggered = true))}
                        }
                    })) { _, _ -> SomeState(event.aggregateId, LocalDate.now().minusDays(1))}
                    .onTimeout("SomeState", event.aggregateId, AnnotationUtil.getSerializationId(StartDateChanged::class))

            with(result.newState!! as SomeState) {
                timeoutTriggered shouldBe true
            }
        }

    }
}