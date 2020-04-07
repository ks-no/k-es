package no.ks.kes.lib

import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import java.time.Instant
import java.util.*

class SagaTest : StringSpec() {
    data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

    @SerializationId("some-id")
    data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>

    @SerializationId("some-other-id")
    data class SomeOtherEvent(override val aggregateId: UUID, override val timestamp: Instant) : Event<SomeAggregate>


    init {
        "test that creating a saga without an initializer throws exception" {
            data class SomeState(val id: UUID)

            val saga = object : Saga<SomeState>(SomeState::class) {}
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "Please define an initializer"
            }
        }

        "test that creating a saga with multiple initializers throws exception" {
            data class SomeState(val id: UUID)

            val saga = object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<SomeEvent>({it.aggregateId}) {SomeState(it.aggregateId)}
                    initOn<SomeOtherEvent>({it.aggregateId}) {SomeState(it.aggregateId)}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "Please specify a single initializer"
            }

        }

        "test that creating a saga which inits on deprecated event throws exception" {
            data class SomeState(val id: UUID)

            @Deprecated(message = "dont use this event")
            data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant): Event<SomeAggregate>

            val saga = object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<SomeEvent>({it.aggregateId}) {SomeState(it.aggregateId)}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "subscribes to the following events which has been deprecated"
            }
        }

        "test that creating a saga which handles an deprecated event throws exception" {
            data class SomeState(val id: UUID)

            @Deprecated(message = "dont use this event")
            @SerializationId("some-deprecated-event")
            data class SomeDeprecatedEvent(override val aggregateId: UUID, override val timestamp: Instant): Event<SomeAggregate>

            val saga = object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<SomeEvent> {setState(SomeState(it.aggregateId))}
                    on<SomeDeprecatedEvent> {setState(SomeState(it.aggregateId))}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "subscribes to the following events which has been deprecated"
            }
        }

        "test that a saga which constructs a timeout on a deprecated event throws exception" {
            data class SomeState(val id: UUID)

            @Deprecated(message = "dont use this event")
            @SerializationId("some-deprecated-event")
            data class SomeDeprecatedEvent(override val aggregateId: UUID, override val timestamp: Instant): Event<SomeAggregate>

            val saga = object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<SomeEvent> {setState(SomeState(it.aggregateId))}
                    createTimeoutOn<SomeDeprecatedEvent>({it.aggregateId}, { e -> Instant.now()}) {setState(SomeState(UUID.randomUUID()))}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "subscribes to the following events which has been deprecated"
            }
        }

        "test that a saga which constructs multiple timeouts on the same event throws exception" {
            data class SomeState(val id: UUID)

            val saga = object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<SomeEvent> {setState(SomeState(it.aggregateId))}
                    createTimeoutOn<SomeOtherEvent>({it.aggregateId}, {e -> Instant.now()}) {setState(SomeState(UUID.randomUUID()))}
                    createTimeoutOn<SomeOtherEvent>({it.aggregateId}, {e -> Instant.now()}) {setState(SomeState(UUID.randomUUID()))}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "The following events occur multiple times in a \"createTimeout\" specification"
            }
        }

        "test that creating a saga with initializer and handler for same event throws exception" {
            data class SomeState(val id: UUID)

            val saga = object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<SomeEvent>({it.aggregateId}) {SomeState(it.aggregateId)}
                    on<SomeEvent>({it.aggregateId}) {state.copy()}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "Please remove duplicates"
            }

        }

        "test that creating a saga with multiple handlers for same event throws exception" {
            data class SomeState(val id: UUID)

            val saga = object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<SomeEvent>({it.aggregateId}) {SomeState(it.aggregateId)}
                    on<SomeOtherEvent>({it.aggregateId}) {state.copy()}
                    on<SomeOtherEvent>({it.aggregateId}) {state.copy()}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "Please remove duplicates"
            }
        }
    }
}