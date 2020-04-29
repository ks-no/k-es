package no.ks.kes.lib

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import java.time.Instant
import java.util.*
import kotlin.random.Random

internal class AggregateConfigurationTest : StringSpec() {

    init {
        "Test that an applied event can initialize an aggregate" {
            data class SomeState(val stateInitialized: Boolean) : Aggregate

            data class SomeInitEvent(override val aggregateId: UUID) : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init<SomeInitEvent> {
                        SomeState(stateInitialized = true)
                    }
                }
            }

            val initializedState = aggregateConfig
                    .getConfiguration { it.simpleName!! }
                    .applyEvent(
                            wrapper = EventWrapper(
                                    event = SomeInitEvent(UUID.randomUUID()),
                                    eventNumber = -1,
                                    serializationId = SomeInitEvent::class.simpleName!!
                            ),
                            currentState = null
                    )

            initializedState!!.stateInitialized shouldBe true
        }

        "Test that an applied event can alter aggregate state"{
            data class SomeState(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            data class SomeInitEvent(override val aggregateId: UUID) : Event<SomeState>

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

            val initializedState = aggregateConfig
                    .getConfiguration { it.simpleName!! }
                    .applyEvent(
                            wrapper = EventWrapper(
                                    event = SomeInitEvent(UUID.randomUUID()),
                                    eventNumber = -1,
                                    serializationId = SomeInitEvent::class.simpleName!!
                            ),
                            currentState = null)

            val updatedState = aggregateConfig
                    .getConfiguration { it.simpleName!! }
                    .applyEvent(
                            EventWrapper(
                                    event = SomeLaterEvent(UUID.randomUUID()),
                                    eventNumber = -1,
                                    serializationId = SomeLaterEvent::class.simpleName!!
                            ),
                            initializedState
                    )

            updatedState!!.stateUpdated shouldBe true
        }

        "Test that an aggregate can have multiple initializers"{
            data class SomeState(val initializedWith: String) : Aggregate

            data class SomeInitEvent(override val aggregateId: UUID) : Event<SomeState>

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

            val initializedState0 = aggregateConfig
                    .getConfiguration { it.simpleName!! }
                    .applyEvent(EventWrapper(SomeInitEvent(UUID.randomUUID()), -1, SomeInitEvent::class.simpleName!!), null)
            initializedState0!!.initializedWith shouldBe "SomeInitEvent"

            val initializedState1 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(SomeOtherInitEvent(UUID.randomUUID()), -1, SomeOtherInitEvent::class.simpleName!!), null)
            initializedState1!!.initializedWith shouldBe "SomeOtherInitEvent"

        }

        "Test that an aggregate can have the same event as an initializer and applicator, and that the correct one is invoked depending on if the aggregate exists or not"{
            data class SomeState(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

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

            val initializedState = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(SomeInitEvent(UUID.randomUUID()), -1, SomeInitEvent::class.simpleName!!), null)
            initializedState!!.stateInitialized shouldBe true
            val updatedState = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(SomeInitEvent(UUID.randomUUID()), -1, SomeInitEvent::class.simpleName!!), initializedState)
            updatedState!!.stateUpdated shouldBe true
        }


        //TODO: Discuss
        "Test that a null state is returned if an \"apply\" event is received by an uninitialized aggregate" {
            data class SomeState(val stateUpdated: Boolean = false) : Aggregate

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

            val derivedState = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(SomeEvent(UUID.randomUUID()), Random.nextLong(), SomeEvent::class.simpleName!!), null)
            derivedState shouldBe null
        }

        "Test that subsequent initializers are ignored if the aggregate is already initialized"{
            data class SomeState(val initializedWith: String) : Aggregate

            data class SomeInitEvent(override val aggregateId: UUID) : Event<SomeState>

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

            val derivedState0 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(SomeInitEvent(UUID.randomUUID()), -1, SomeInitEvent::class.simpleName!!), null)
            derivedState0!!.initializedWith shouldBe "SomeInitEvent"

            val derivedState1 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(SomeOtherInitEvent(UUID.randomUUID()), -1, SomeOtherInitEvent::class.simpleName!!), derivedState0)
            derivedState1!!.initializedWith shouldBe "SomeInitEvent"

        }
    }
}