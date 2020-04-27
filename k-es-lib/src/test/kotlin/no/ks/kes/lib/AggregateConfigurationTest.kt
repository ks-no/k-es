package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import java.util.*
import kotlin.random.Random

internal class AggregateConfigurationTest : StringSpec() {

    init {
        "Test that an applied event can initialize an aggregate" {
            data class SomeState(val stateInitialized: Boolean) : Aggregate

            @SerializationId("some-id")
            data class SomeInitEvent(override val aggregateId: UUID) : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init<SomeInitEvent> {
                        SomeState(stateInitialized = true)
                    }
                }
            }

            val initializedState = aggregateConfig.applyEvent(EventWrapper(SomeInitEvent(UUID.randomUUID()), -1), null)

            initializedState!!.stateInitialized shouldBe true
        }

        "Test that an applied event can alter aggregate state"{
            data class SomeState(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            @SerializationId("some-id")
            data class SomeInitEvent(override val aggregateId: UUID) : Event<SomeState>

            @SerializationId("some-other-id")
            data class SomeLaterEvent(override val aggregateId: UUID) : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init<SomeInitEvent> {
                        SomeState(
                                stateInitialized = true
                        )
                    }

                    apply<SomeLaterEvent> {
                        copy(
                                stateUpdated = true
                        )
                    }
                }
            }

            val initializedState = aggregateConfig.applyEvent(EventWrapper(SomeInitEvent(UUID.randomUUID()), -1), null)
            val updatedState = aggregateConfig.applyEvent(EventWrapper(SomeLaterEvent(UUID.randomUUID()), -1), initializedState)

            updatedState!!.stateUpdated shouldBe true
        }

        "Test that an aggregate can have multiple initializers"{
            data class SomeState(val initializedWith: String) : Aggregate

            @SerializationId("some-id")
            data class SomeInitEvent(override val aggregateId: UUID) : Event<SomeState>

            @SerializationId("some-other-id")
            data class SomeOtherInitEvent(override val aggregateId: UUID) : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init<SomeInitEvent> {
                        SomeState(
                                initializedWith = "SomeInitEvent"
                        )
                    }

                    init<SomeOtherInitEvent> {
                        SomeState(
                                initializedWith = "SomeOtherInitEvent"
                        )
                    }
                }
            }

            val initializedState0 = aggregateConfig.applyEvent(EventWrapper(SomeInitEvent(UUID.randomUUID()), -1), null)
            initializedState0!!.initializedWith shouldBe "SomeInitEvent"

            val initializedState1 = aggregateConfig.applyEvent(EventWrapper(SomeOtherInitEvent(UUID.randomUUID()), -1), null)
            initializedState1!!.initializedWith shouldBe "SomeOtherInitEvent"

        }

        "Test that an aggregate can have the same event as an initializer and applicator, and that the correct one is invoked depending on if the aggregate exists or not"{
            data class SomeState(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            @SerializationId("some-id")
            data class SomeInitEvent(override val aggregateId: UUID) : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init<SomeInitEvent> {
                        SomeState(
                                stateInitialized = true
                        )
                    }

                    apply<SomeInitEvent> {
                        copy(
                                stateUpdated = true
                        )
                    }
                }
            }

            val initializedState = aggregateConfig.applyEvent(EventWrapper(SomeInitEvent(UUID.randomUUID()), -1), null)
            initializedState!!.stateInitialized shouldBe true
            val updatedState = aggregateConfig.applyEvent(EventWrapper(SomeInitEvent(UUID.randomUUID()), -1), initializedState)
            updatedState!!.stateUpdated shouldBe true
        }


        //TODO: Discuss
        "Test that a null state is returned if an \"apply\" event is received by an uninitialized aggregate" {
            data class SomeState(val stateUpdated: Boolean = false) : Aggregate

            @SerializationId("some-id")
            data class SomeEvent(override val aggregateId: UUID) : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    apply<SomeEvent> {
                        copy(
                                stateUpdated = true
                        )
                    }
                }
            }

            val derivedState = aggregateConfig.applyEvent(EventWrapper(SomeEvent(UUID.randomUUID()), Random.nextLong()), null)
            derivedState shouldBe null
        }

        "Test that subsequent initializers are ignored if the aggregate is already initialized"{
            data class SomeState(val initializedWith: String) : Aggregate

            @SerializationId("some-id")
            data class SomeInitEvent(override val aggregateId: UUID) : Event<SomeState>

            @SerializationId("some-other-id")
            data class SomeOtherInitEvent(override val aggregateId: UUID) : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init<SomeInitEvent> {
                        SomeState(
                                initializedWith = "SomeInitEvent"
                        )
                    }

                    init<SomeOtherInitEvent> {
                        SomeState(
                                initializedWith = "SomeOtherInitEvent"
                        )
                    }
                }
            }

            val derivedState0 = aggregateConfig.applyEvent(EventWrapper(SomeInitEvent(UUID.randomUUID()), -1), null)
            derivedState0!!.initializedWith shouldBe "SomeInitEvent"

            val derivedState1 = aggregateConfig.applyEvent(EventWrapper(SomeOtherInitEvent(UUID.randomUUID()), -1), derivedState0)
            derivedState1!!.initializedWith shouldBe "SomeInitEvent"

        }
    }
}