package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import java.time.Instant
import java.util.*

class SagaEventHandlingTest : StringSpec() {
    private data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    private data class SomeState(val id: UUID, val updated: Boolean = false)

    private data class SomeCmd(override val aggregateId: UUID): Cmd<SomeAggregate>

    @SerializationId("some-id")
    private data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>

    @Deprecated(message = "dont use this event")
    @SerializationId("some-deprecated-event")
    private data class SomeDeprecatedEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>

    init {
        "test that a saga with no pre-existing state can be initialized" {
                val event = SomeEvent(UUID.randomUUID(), Instant.now())
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                    init {
                        init<SomeEvent>({ it.aggregateId }) { setState(SomeState(it.aggregateId)) }
                    }
                }.handleEvent(EventWrapper(event, -1)) { _, _ -> null}.apply {
                    with(this as SagaRepository.Operation.Insert){
                        correlationId shouldBe event.aggregateId
                        serializationId shouldBe sagaSerializationId
                        newState shouldBe SomeState(event.aggregateId)
                        commands shouldBe emptyList()
                    }
                }
        }

        "test that a saga which already exists will not be initialized" {
            val event = SomeEvent(UUID.randomUUID(), Instant.now())
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    init<SomeEvent>({ it.aggregateId }) { setState(SomeState(it.aggregateId)) }
                }
            }.handleEvent(EventWrapper(event, -1)) { id, _ -> SomeState(id)}.apply {
                this shouldBe null
            }
        }

        "test that a saga can dispatch commands during initialization" {
            val event = SomeEvent(UUID.randomUUID(), Instant.now())
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    init<SomeEvent>({ it.aggregateId }) {
                        setState(SomeState(it.aggregateId))
                        dispatch(SomeCmd(it.aggregateId))
                    }
                }
            }.handleEvent(EventWrapper(event, -1)) { _, _ -> null}.apply {
                with(this as SagaRepository.Operation.Insert){
                    correlationId shouldBe event.aggregateId
                    serializationId shouldBe sagaSerializationId
                    newState shouldBe SomeState(event.aggregateId)
                    commands.single() shouldBe SomeCmd(event.aggregateId)
                }
            }
        }

        "test that we can mutate state while handling events on existing saga" {
            val event = SomeEvent(UUID.randomUUID(), Instant.now())
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    apply<SomeEvent>({ it.aggregateId }) { setState(state.copy(updated = true)) }
                }
            }.handleEvent(EventWrapper(event, 0)) { id, _ -> SomeState(id)}.apply {
                with(this as SagaRepository.Operation.SagaUpdate){
                    correlationId shouldBe event.aggregateId
                    serializationId shouldBe sagaSerializationId
                    newState shouldBe SomeState(event.aggregateId, true)
                    commands shouldBe emptyList()
                }
            }
        }

        "test that we can dispatch commands while handling event on existing saga" {
            val event = SomeEvent(UUID.randomUUID(), Instant.now())
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    apply<SomeEvent>({ it.aggregateId }) { dispatch(SomeCmd(it.aggregateId)) }
                }
            }.handleEvent(EventWrapper(event, 0)) { id, _ -> SomeState(id)}.apply {
                with(this as SagaRepository.Operation.SagaUpdate){
                    correlationId shouldBe event.aggregateId
                    serializationId shouldBe sagaSerializationId
                    newState shouldBe null
                    commands.single() shouldBe SomeCmd(event.aggregateId)
                }
            }
        }

        "test that we can create timeouts while handling event on existing saga" {
            val event = SomeEvent(UUID.randomUUID(), Instant.now())
            val sagaSerializationId = "SomeSaga"
            val timeoutAt = Instant.now()

            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    timeout<SomeEvent>({ it.aggregateId }, { timeoutAt }) {
                        dispatch(SomeCmd(state.id)) }
                }
            }.handleEvent(EventWrapper(event, 0)) { id, _ -> SomeState(id)}.apply {
                with(this as SagaRepository.Operation.SagaUpdate){
                    correlationId shouldBe event.aggregateId
                    serializationId shouldBe sagaSerializationId
                    newState shouldBe null
                    timeouts.single() shouldBe Saga.Timeout(timeoutAt, SomeEvent::class.serializationId)
                }
            }
        }

        "test that timeout can trigger command dispatch" {
            val event = SomeEvent(UUID.randomUUID(), Instant.now())
            val sagaSerializationId = "SomeSaga"
            val timeoutAt = Instant.now()

            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    timeout<SomeEvent>({ it.aggregateId }, { timeoutAt }) {
                        dispatch(SomeCmd(state.id)) }
                }
            }.handleTimeout(SagaRepository.Timeout(
                    sagaCorrelationId = event.aggregateId,
                    sagaSerializationId = sagaSerializationId,
                    timeoutId = SomeEvent::class.serializationId
            )) { id, _ -> SomeState(id)}.apply {
                with(this as SagaRepository.Operation.SagaUpdate){
                    correlationId shouldBe event.aggregateId
                    serializationId shouldBe sagaSerializationId
                    newState shouldBe null
                    commands.single() shouldBe SomeCmd(event.aggregateId)
                }
            }
        }

        "test that the same event can be used as an init and apply, and that the init is executed if the saga does not exist" {
            val event = SomeEvent(UUID.randomUUID(), Instant.now())
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    init<SomeEvent>({ it.aggregateId }) { setState(SomeState(it.aggregateId)) }
                    apply<SomeEvent>({ it.aggregateId }) { setState(state.copy(updated = true)) }
                }
            }.handleEvent(EventWrapper(event, 0)) { id, _ -> null}.apply {
                with(this as SagaRepository.Operation.Insert){
                    correlationId shouldBe event.aggregateId
                    serializationId shouldBe sagaSerializationId
                    newState shouldBe SomeState(event.aggregateId)
                    commands shouldBe emptyList()
                }
            }
        }

        "test that the same event can be used as an init and apply, and that the apply is executed if the saga exists" {
            val event = SomeEvent(UUID.randomUUID(), Instant.now())
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    init<SomeEvent>({ it.aggregateId }) { setState(SomeState(it.aggregateId)) }
                    apply<SomeEvent>({ it.aggregateId }) { setState(state.copy(updated = true)) }
                }
            }.handleEvent(EventWrapper(event, 0)) { id, _ -> SomeState(id)}.apply {
                with(this as SagaRepository.Operation.SagaUpdate){
                    correlationId shouldBe event.aggregateId
                    serializationId shouldBe sagaSerializationId
                    newState shouldBe SomeState(event.aggregateId, true)
                    commands shouldBe emptyList()
                }
            }
        }
    }
}