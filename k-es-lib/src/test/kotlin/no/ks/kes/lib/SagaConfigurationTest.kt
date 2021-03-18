package no.ks.kes.lib

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.string.shouldContain
import java.time.Instant
import java.util.*

class SagaConfigurationTest : StringSpec() {
    private data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    private data class SomeState(val id: UUID)

    private data class SomeEvent(val aggregateId: UUID) : Event<SomeAggregate>

    @Deprecated(message = "dont use this event")
    private data class SomeDeprecatedEvent(val aggregateId: UUID) : Event<SomeAggregate>

    init {

        "test that creating a saga with multiple initializers throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "SomeSaga") {
                    init {
                        init({ someEvent: SomeEvent, aggregateId: UUID ->  aggregateId }) { someEvent: SomeEvent, aggregateId: UUID ->  SomeState(aggregateId) }
                        init({ someEvent: SomeEvent, aggregateId: UUID ->  aggregateId }) { someEvent: SomeEvent, aggregateId: UUID ->  SomeState(aggregateId) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "There are multiple \"init\" configurations for event-type(s)"

        }

        "test that creating a saga which inits on deprecated event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "SomeSaga") {
                    init {
                        init({ someEvent: SomeDeprecatedEvent, aggregateId: UUID ->  aggregateId }) { someEvent: SomeDeprecatedEvent, aggregateId: UUID ->  SomeState(aggregateId) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "handles deprecated event"

        }

        "test that creating a saga which applies a deprecated event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "SomeSaga") {
                    init {
                        apply({ someEvent: SomeDeprecatedEvent, aggregateId: UUID ->  aggregateId }) { someEvent: SomeDeprecatedEvent, aggregateId: UUID ->  SomeState(aggregateId) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "handles deprecated event"

        }

        "test that a saga which constructs a timeout on a deprecated event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "someSaga") {
                    init {
                        init({ someEvent: SomeEvent, aggregateId: UUID ->  aggregateId }) { someEvent: SomeEvent, aggregateId: UUID ->  setState(SomeState(aggregateId)) }
                        timeout({ someEvent: SomeDeprecatedEvent, aggregateId: UUID ->  aggregateId }, { Instant.now() }) { setState(SomeState(UUID.randomUUID())) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "handles deprecated event"

        }

        "test that a saga which constructs a timeout and an apply on the same event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "someSaga") {
                    init {
                        apply { e: SomeEvent, aggregateId: UUID -> setState(SomeState(aggregateId)) }
                        timeout({ someEvent: SomeEvent, aggregateId: UUID ->  aggregateId }, { Instant.now() }) { setState(SomeState(UUID.randomUUID())) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "There are multiple \"apply/timeout\" configurations for event-type(s)"

        }

        "test that a saga which constructs multiple applys on the same event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "someSaga") {
                    init {
                        apply<SomeEvent> { e: SomeEvent, aggregateId: UUID -> setState(SomeState(aggregateId)) }
                        apply<SomeEvent> { e: SomeEvent, aggregateId: UUID -> setState(SomeState(aggregateId)) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "There are multiple \"apply/timeout\" configurations for event-type(s)"
        }
    }
}