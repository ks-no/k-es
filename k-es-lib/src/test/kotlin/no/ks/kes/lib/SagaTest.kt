package no.ks.kes.lib

import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import no.ks.kes.lib.testdomain.ConfidentialityAgreementRejected
import no.ks.kes.lib.testdomain.Employee
import no.ks.kes.lib.testdomain.Hired
import java.time.Instant
import java.util.*

class SagaTest : StringSpec() {
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
                    initOn<Hired>({it.aggregateId}) {SomeState(it.aggregateId)}
                    initOn<ConfidentialityAgreementRejected>({it.aggregateId}) {SomeState(it.aggregateId)}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "Please specify a single initializer"
            }

        }

        "test that creating a saga which inits on deprecated event throws exception" {
            data class SomeState(val id: UUID)

            @Deprecated(message = "dont use this event")
            data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant): Event<Employee>

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
            @SerializationId("SomeEvent")
            data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant): Event<Employee>

            val saga = object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<Hired> {setState(SomeState(it.aggregateId))}
                    on<SomeEvent> {setState(SomeState(it.aggregateId))}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "subscribes to the following events which has been deprecated"
            }
        }

        "test that a saga which constructs a timeout on a deprecated event throws exception" {
            data class SomeState(val id: UUID)

            @Deprecated(message = "dont use this event")
            @SerializationId("SomeEvent")
            data class SomeEvent(override val aggregateId: UUID, override val timestamp: Instant): Event<Employee>

            val saga = object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<Hired> {setState(SomeState(it.aggregateId))}
                    createTimeoutOn<SomeEvent>({it.aggregateId}, {e -> Instant.now()}) {setState(SomeState(UUID.randomUUID()))}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "subscribes to the following events which has been deprecated"
            }
        }

        "test that creating a saga with initializer and handler for same event throws exception" {
            data class SomeState(val id: UUID)

            val saga = object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<Hired>({it.aggregateId}) {SomeState(it.aggregateId)}
                    on<Hired>({it.aggregateId}) {state.copy()}
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
                    initOn<Hired>({it.aggregateId}) {SomeState(it.aggregateId)}
                    on<ConfidentialityAgreementRejected>({it.aggregateId}) {state.copy()}
                    on<ConfidentialityAgreementRejected>({it.aggregateId}) {state.copy()}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "Please remove duplicates"
            }
        }
    }
}