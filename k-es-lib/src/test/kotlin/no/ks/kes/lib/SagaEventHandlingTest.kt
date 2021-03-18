package no.ks.kes.lib

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import java.time.Instant
import java.util.*

class SagaEventHandlingTest : StringSpec() {
    private data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    private data class SomeState(val id: UUID, val updated: Boolean = false)

    private data class SomeCmd(override val aggregateId: UUID) : Cmd<SomeAggregate>

    private data class SomeEvent(val aggregateId: UUID) : Event<SomeAggregate>

    @Deprecated(message = "dont use this event")
    private data class SomeDeprecatedEvent(val aggregateId: UUID) : Event<SomeAggregate>

    init {
        "test that a saga with no pre-existing state can be initialized" {
            val aggregateId = UUID.randomUUID()
            val event = SomeEvent(aggregateId)
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    init({ someEvent: SomeEvent, aggregateId: UUID -> aggregateId }) { someEvent: SomeEvent, aggregateId: UUID -> setState(SomeState(aggregateId)) }
                }
            }.getConfiguration { it.simpleName!! }
                    .handleEvent(EventWrapper(aggregateId, event,null, -1, event::class.simpleName!!)) { _, _ -> null }
                    .run {
                        with(this as SagaRepository.Operation.Insert) {
                            correlationId shouldBe event.aggregateId
                            serializationId shouldBe sagaSerializationId
                            newState shouldBe SomeState(event.aggregateId)
                            commands shouldBe emptyList()
                        }
                    }
        }

        "test that a saga which already exists will not be initialized" {
            val aggregateId = UUID.randomUUID()
            val event = SomeEvent(aggregateId)
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    init({ someEvent: SomeEvent, aggregateId: UUID -> aggregateId }) { someEvent: SomeEvent, aggregateId: UUID -> setState(SomeState(aggregateId)) }
                }
            }.getConfiguration { it.simpleName!! }
                    .handleEvent(EventWrapper(aggregateId,event,null, -1, event::class.simpleName!!)) { id, _ -> SomeState(id) }.apply {
                        this shouldBe null
                    }
        }

        "test that a saga can dispatch commands during initialization" {
            val aggregateId = UUID.randomUUID()
            val event = SomeEvent(aggregateId)
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    init({ someEvent: SomeEvent, aggregateId: UUID -> aggregateId }) { someEvent: SomeEvent, aggregateId: UUID ->
                        setState(SomeState(aggregateId))
                        dispatch(SomeCmd(aggregateId))
                    }
                }
            }.getConfiguration { it.simpleName!! }
                    .handleEvent(EventWrapper(aggregateId,event,null, -1, event::class.simpleName!!)) { _, _ -> null }.apply {
                        with(this as SagaRepository.Operation.Insert) {
                            correlationId shouldBe event.aggregateId
                            serializationId shouldBe sagaSerializationId
                            newState shouldBe SomeState(event.aggregateId)
                            commands.single() shouldBe SomeCmd(event.aggregateId)
                        }
                    }
        }

        "test that we can mutate state while handling events on existing saga" {
            val aggregateId = UUID.randomUUID()
            val event = SomeEvent(aggregateId)
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    apply({ someEvent: SomeEvent, aggregateId: UUID -> aggregateId }) { someEvent: SomeEvent, aggregateId: UUID -> setState(state.copy(updated = true)) }
                }
            }.getConfiguration { it.simpleName!! }
                    .handleEvent(EventWrapper(aggregateId,event,null, 0, event::class.simpleName!!)) { id, _ -> SomeState(id) }.apply {
                        with(this as SagaRepository.Operation.SagaUpdate) {
                            correlationId shouldBe event.aggregateId
                            serializationId shouldBe sagaSerializationId
                            newState shouldBe SomeState(event.aggregateId, true)
                            commands shouldBe emptyList()
                        }
                    }
        }

        "test that we can dispatch commands while handling event on existing saga" {
            val aggregateId = UUID.randomUUID()
            val event = SomeEvent(aggregateId)
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    apply({ someEvent: SomeEvent, aggregateId: UUID -> aggregateId }) { someEvent: SomeEvent, aggregateId: UUID -> dispatch(SomeCmd(aggregateId)) }
                }
            }.getConfiguration { it.simpleName!! }
                    .handleEvent(EventWrapper(aggregateId,event,null, 0, event::class.simpleName!!)) { id, _ -> SomeState(id) }.apply {
                        with(this as SagaRepository.Operation.SagaUpdate) {
                            correlationId shouldBe event.aggregateId
                            serializationId shouldBe sagaSerializationId
                            newState shouldBe null
                            commands.single() shouldBe SomeCmd(event.aggregateId)
                        }
                    }
        }

        "test that we can create timeouts while handling event on existing saga" {
            val aggregateId = UUID.randomUUID()
            val event = SomeEvent(aggregateId)
            val sagaSerializationId = "SomeSaga"
            val timeoutAt = Instant.now()

            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    timeout({ someEvent: SomeEvent, aggregateId: UUID -> aggregateId }, { timeoutAt }) {
                        dispatch(SomeCmd(state.id))
                    }
                }
            }.getConfiguration { it.simpleName!! }
                    .handleEvent(EventWrapper(aggregateId,event,null, 0, event::class.simpleName!!)) { id, _ -> SomeState(id) }
                    .run {
                        with(this as SagaRepository.Operation.SagaUpdate) {
                            correlationId shouldBe event.aggregateId
                            serializationId shouldBe sagaSerializationId
                            newState shouldBe null
                            timeouts.single() shouldBe Saga.Timeout(timeoutAt, SomeEvent::class.simpleName!!)
                        }
                    }
        }

        "test that timeout can trigger command dispatch" {
            val aggregateId = UUID.randomUUID()
            val event = SomeEvent(aggregateId)
            val sagaSerializationId = "SomeSaga"
            val timeoutAt = Instant.now()

            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    timeout({ someEvent: SomeEvent, aggregateId: UUID -> aggregateId }, { timeoutAt }) {
                        dispatch(SomeCmd(state.id))
                    }
                }
            }.getConfiguration { it.simpleName!! }
                    .handleTimeout(SagaRepository.Timeout(
                            sagaCorrelationId = event.aggregateId,
                            sagaSerializationId = sagaSerializationId,
                            timeoutId = SomeEvent::class.simpleName!!
                    )) { id, _ -> SomeState(id) }
                    .run {
                        with(this as SagaRepository.Operation.SagaUpdate) {
                            correlationId shouldBe event.aggregateId
                            serializationId shouldBe sagaSerializationId
                            newState shouldBe null
                            commands.single() shouldBe SomeCmd(event.aggregateId)
                        }
                    }
        }

        "test that the same event can be used as an init and apply, and that the init is executed if the saga does not exist" {
            val aggregateId = UUID.randomUUID()
            val event = SomeEvent(aggregateId)
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    init({ someEvent: SomeEvent, aggregateId: UUID -> aggregateId }) { someEvent: SomeEvent, aggregateId: UUID -> setState(SomeState(aggregateId)) }
                    apply({ someEvent: SomeEvent, aggregateId: UUID -> aggregateId }) { someEvent: SomeEvent, aggregateId: UUID -> setState(state.copy(updated = true)) }
                }
            }.getConfiguration { it.simpleName!! }
                    .handleEvent(EventWrapper(aggregateId,event,null, 0, event::class.simpleName!!)) { _, _ -> null }
                    .run {
                        with(this as SagaRepository.Operation.Insert) {
                            correlationId shouldBe event.aggregateId
                            serializationId shouldBe sagaSerializationId
                            newState shouldBe SomeState(event.aggregateId)
                            commands shouldBe emptyList()
                        }
                    }
        }

        "test that the same event can be used as an init and apply, and that the apply is executed if the saga exists" {
            val aggregateId = UUID.randomUUID()
            val event = SomeEvent(aggregateId)
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    init({ someEvent: SomeEvent, aggregateId: UUID -> aggregateId }) { someEvent: SomeEvent, aggregateId: UUID -> setState(SomeState(aggregateId)) }
                    apply({ someEvent: SomeEvent, aggregateId: UUID -> aggregateId }) { someEvent: SomeEvent, aggregateId: UUID -> setState(state.copy(updated = true)) }
                }
            }.getConfiguration { it.simpleName!! }
                    .handleEvent(EventWrapper(aggregateId,event,null, 0, event::class.simpleName!!)) { id, _ -> SomeState(id) }
                    .run {
                        with(this as SagaRepository.Operation.SagaUpdate) {
                            correlationId shouldBe event.aggregateId
                            serializationId shouldBe sagaSerializationId
                            newState shouldBe SomeState(event.aggregateId, true)
                            commands shouldBe emptyList()
                        }
                    }
        }
    }
}