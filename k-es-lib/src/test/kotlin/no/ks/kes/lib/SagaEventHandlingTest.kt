package no.ks.kes.lib

import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import java.time.Instant
import java.util.*

class SagaEventHandlingTest : StringSpec() {
    private data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    private data class SomeState(val id: UUID, val updated: Boolean = false)

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
                    with(this as SagaRepository.SagaUpsert.SagaInsert){
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

        "test that we can apply changes to an existing saga" {
            val event = SomeEvent(UUID.randomUUID(), Instant.now())
            val sagaSerializationId = "SomeSaga"
            object : Saga<SomeState>(SomeState::class, sagaSerializationId) {
                init {
                    apply<SomeEvent>({ it.aggregateId }) { setState(state.copy(updated = true)) }
                }
            }.handleEvent(EventWrapper(event, 0)) { id, _ -> SomeState(id)}.apply {
                with(this as SagaRepository.SagaUpsert.SagaUpdate){
                    correlationId shouldBe event.aggregateId
                    serializationId shouldBe sagaSerializationId
                    newState shouldBe SomeState(event.aggregateId, true)
                    commands shouldBe emptyList()
                }
            }
        }
    }
}