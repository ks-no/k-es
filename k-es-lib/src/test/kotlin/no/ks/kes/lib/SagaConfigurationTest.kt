package no.ks.kes.lib

import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import java.time.Instant
import java.util.*

class SagaConfigurationTest : StringSpec() {
    private data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    private data class SomeState(val id: UUID)

    @SerializationId("some-id")
    private data class SomeEvent(override val aggregateId: UUID) : Event<SomeAggregate>

    @Deprecated(message = "dont use this event")
    @SerializationId("some-deprecated-event")
    private data class SomeDeprecatedEvent(override val aggregateId: UUID) : Event<SomeAggregate>

    init {

        "test that creating a saga with multiple initializers throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "SomeSaga") {
                    init {
                        init<SomeEvent>({ it.aggregateId }) { SomeState(it.aggregateId) }
                        init<SomeEvent>({ it.aggregateId }) { SomeState(it.aggregateId) }
                    }
                }
            }.message shouldContain "Duplicate init handler"

        }

        "test that creating a saga which inits on deprecated event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "SomeSaga") {
                    init {
                        init<SomeDeprecatedEvent>({ it.aggregateId }) { SomeState(it.aggregateId) }
                    }
                }
            }.message shouldContain "handles deprecated event"

        }

        "test that creating a saga which applies a deprecated event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "SomeSaga") {
                    init {
                        apply<SomeDeprecatedEvent>({ it.aggregateId }) { SomeState(it.aggregateId) }
                    }
                }
            }.message shouldContain "handles deprecated event"

        }

        "test that a saga which constructs a timeout on a deprecated event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "someSaga") {
                    init {
                        init<SomeEvent> { setState(SomeState(it.aggregateId)) }
                        timeout<SomeDeprecatedEvent>({ it.aggregateId }, { e -> Instant.now() }) { setState(SomeState(UUID.randomUUID())) }
                    }
                }
            }.message shouldContain "handles deprecated event"

        }

        "test that a saga which constructs a timeout and an apply on the same event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "someSaga") {
                    init {
                        apply<SomeEvent> { setState(SomeState(it.aggregateId)) }
                        timeout<SomeEvent>({ it.aggregateId }, { e -> Instant.now() }) { setState(SomeState(UUID.randomUUID())) }
                    }
                }
            }.message shouldContain "Duplicate apply/timeout handler for event"

        }

        "test that a saga which constructs multiple applys on the same event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "someSaga") {
                    init {
                        apply<SomeEvent> { setState(SomeState(it.aggregateId)) }
                        apply<SomeEvent> { setState(SomeState(it.aggregateId)) }
                    }
                }
            }.message shouldContain "Duplicate apply/timeout handler for event"
        }
    }
}