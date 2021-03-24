package no.ks.kes.lib

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.string.shouldContain
import java.time.Instant
import java.util.*

class SagaConfigurationTest : StringSpec() {
    private data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    private data class SomeState(val id: UUID)

    private data class SomeEventData(val aggregateId: UUID) : EventData<SomeAggregate>

    @Deprecated(message = "dont use this event")
    private data class SomeDeprecatedEvent(val aggregateId: UUID) : EventData<SomeAggregate>

    init {

        "test that creating a saga with multiple initializers throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "SomeSaga") {
                    init {
                        init({ _: SomeEventData, aggregateId: UUID ->  aggregateId }) { _: SomeEventData, aggregateId: UUID ->  SomeState(aggregateId) }
                        init({ _: SomeEventData, aggregateId: UUID ->  aggregateId }) { _: SomeEventData, aggregateId: UUID ->  SomeState(aggregateId) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "There are multiple \"init\" configurations for event-type(s)"

        }

        "test that creating a saga which inits on deprecated event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "SomeSaga") {
                    init {
                        init({ _: SomeDeprecatedEvent, aggregateId: UUID ->  aggregateId }) { _: SomeDeprecatedEvent, aggregateId: UUID ->  SomeState(aggregateId) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "handles deprecated event"

        }

        "test that creating a saga which applies a deprecated event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "SomeSaga") {
                    init {
                        apply({ _: SomeDeprecatedEvent, aggregateId: UUID ->  aggregateId }) { _: SomeDeprecatedEvent, aggregateId: UUID ->  SomeState(aggregateId) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "handles deprecated event"

        }

        "test that a saga which constructs a timeout on a deprecated event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "someSaga") {
                    init {
                        init({ _: SomeEventData, aggregateId: UUID ->  aggregateId }) { _: SomeEventData, aggregateId: UUID ->  setState(SomeState(aggregateId)) }
                        timeout({ _: SomeDeprecatedEvent, aggregateId: UUID ->  aggregateId }, { Instant.now() }) { setState(SomeState(UUID.randomUUID())) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "handles deprecated event"

        }

        "test that a saga which constructs a timeout and an apply on the same event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "someSaga") {
                    init {
                        apply { _: SomeEventData, aggregateId: UUID -> setState(SomeState(aggregateId)) }
                        timeout({ _: SomeEventData, aggregateId: UUID ->  aggregateId }, { Instant.now() }) { setState(SomeState(UUID.randomUUID())) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "There are multiple \"apply/timeout\" configurations for event-type(s)"

        }

        "test that a saga which constructs multiple applys on the same event throws exception" {
            shouldThrow<IllegalStateException> {
                object : Saga<SomeState>(SomeState::class, "someSaga") {
                    init {
                        apply { _: SomeEventData, aggregateId: UUID -> setState(SomeState(aggregateId)) }
                        apply { _: SomeEventData, aggregateId: UUID -> setState(SomeState(aggregateId)) }
                    }
                }.getConfiguration { it.simpleName!! }
            }.message shouldContain "There are multiple \"apply/timeout\" configurations for event-type(s)"
        }
    }
}