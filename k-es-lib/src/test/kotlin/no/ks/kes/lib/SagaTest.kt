package no.ks.kes.lib

import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import no.ks.kes.lib.testdomain.ConfidentialityAgreementRejected
import no.ks.kes.lib.testdomain.HiredEvent
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
                    initOn<HiredEvent>({aggregateId}) {SomeState(it.aggregateId)}
                    initOn<ConfidentialityAgreementRejected>({aggregateId}) {SomeState(it.aggregateId)}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "Please specify a single initializer"
            }

        }

        "test that creating a saga with initializer and handler for same event throws exception" {
            data class SomeState(val id: UUID)

            val saga = object : Saga<SomeState>(SomeState::class) {
                init {
                    initOn<HiredEvent>({aggregateId}) {SomeState(it.aggregateId)}
                    on<HiredEvent>({aggregateId}) {copy()}
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
                    initOn<HiredEvent>({aggregateId}) {SomeState(it.aggregateId)}
                    on<ConfidentialityAgreementRejected>({aggregateId}) {copy()}
                    on<ConfidentialityAgreementRejected>({aggregateId}) {copy()}
                }
            }
            shouldThrow<IllegalStateException> { saga.getConfiguration() }.apply {
                message shouldContain  "Please remove duplicates"
            }

        }

    }
}