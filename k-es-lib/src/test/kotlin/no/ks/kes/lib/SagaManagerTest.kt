package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import java.time.Instant
import java.time.LocalDate
import java.util.*

class SagaManagerTest : StringSpec() {
    data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    @SerializationId("some-id")
    data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>

    @SerializationId("some-other-id")
    data class SomeOtherEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>

    init {
        "test that the saga manager should extract a Insert with a new state when the saga is initialized" {
            data class SomeState(val id: UUID)

            val saga = @SerializationId("SomeState") object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<SomeEvent>({ it.aggregateId }) {
                        setState(SomeState(it.aggregateId))
                    }
                }
            }

            val sagaManager = Sagas(setOf(saga)) { _, _ -> null }

            val event = SomeEvent(UUID.randomUUID(), Instant.now())
            val onEvent = sagaManager.onEvent(EventWrapper(event, 0L))

            with(onEvent.single() as SagaRepository.SagaUpsert.SagaInsert) {
                newState shouldBe SomeState(event.aggregateId)
            }
        }

        "test that the saga manager should extract a Update with a new state when onEvent is invoked after initialization" {
            data class SomeState(val id: UUID, val updated: Boolean = false)

            val event = SomeOtherEvent(UUID.randomUUID(), Instant.now())
            val onEvent = Sagas(
                    setOf<Saga<SomeState>>(@SerializationId("SomeState") object : Saga<SomeState>(SomeState::class) {
                        init {
                            initOn<SomeEvent> {
                                setState(SomeState(it.aggregateId))
                            }

                            on<SomeOtherEvent> {
                                setState(state.copy(updated = true))
                            }
                        }
                    })) { _, _ -> SomeState(event.aggregateId) }
                    .onEvent(EventWrapper(event, 0L))

            with(onEvent.single() as SagaRepository.SagaUpsert.SagaUpdate) {
                newState shouldBe SomeState(event.aggregateId, true)
            }
        }

        "test that the saga manager should extract \"create timeout\" when one is configured as an event reaction" {
            data class SomeState(val id: UUID, val updated: Boolean = false)

            val event = SomeOtherEvent(UUID.randomUUID(), Instant.now())

            val timeoutTime = Instant.now()
            val onEvent = Sagas(
                    setOf<Saga<SomeState>>(@SerializationId("SomeState") object : Saga<SomeState>(SomeState::class) {
                        init {
                            initOn<SomeEvent> {
                                setState(SomeState(it.aggregateId))
                            }

                            createTimeoutOn<SomeOtherEvent>({it.aggregateId}, {timeoutTime}) {}
                        }
                    })) { _, _ -> SomeState(event.aggregateId)}
                    .onEvent(EventWrapper(event, 0L))

            with(onEvent.single() as SagaRepository.SagaUpsert.SagaUpdate) {
                timeouts.single() shouldBe Saga.Timeout(timeoutTime, AnnotationUtil.getSerializationId(SomeOtherEvent::class))
            }
        }

        "test that the saga manager should execute the timeout handler when a triggered timeout arrives" {
            data class SomeState(val id: UUID, val timeoutTriggered: Boolean = false)

            val event = SomeOtherEvent(UUID.randomUUID(), Instant.now())

            val timeoutTime = Instant.now()
            val result = Sagas(
                    setOf<Saga<SomeState>>(@SerializationId("SomeState") object : Saga<SomeState>(SomeState::class) {
                        init {
                            initOn<SomeEvent> {
                                setState(SomeState(it.aggregateId))
                            }

                            createTimeoutOn<SomeOtherEvent>({it.aggregateId}, {timeoutTime}) {setState(state.copy(timeoutTriggered = true))}
                        }
                    })) { _, _ -> SomeState(event.aggregateId)}
                    .onTimeout("SomeState", event.aggregateId, AnnotationUtil.getSerializationId(SomeOtherEvent::class))

            with(result.newState!! as SomeState) {
                timeoutTriggered shouldBe true
            }
        }

    }
}