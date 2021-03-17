package no.ks.kes.lib

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import java.util.*
import kotlin.random.Random

internal class AggregateConfigurationTest : StringSpec() {

    init {
        "Test that an applied event can initialize an aggregate" {
            data class SomeState(val stateInitialized: Boolean) : Aggregate

            class SomeInitEvent : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init { someInitEvent: SomeInitEvent, aggregateId: UUID ->
                        SomeState(stateInitialized = true)
                    }
                }
            }

            val initializedState = aggregateConfig
                    .getConfiguration { it.simpleName!! }
                    .applyEvent(
                            wrapper = EventWrapper(
                                    aggregateId = UUID.randomUUID(),
                                    event = SomeInitEvent(),
                                    eventNumber = -1,
                                    serializationId = SomeInitEvent::class.simpleName!!
                            ),
                            currentState = null
                    )

            initializedState!!.stateInitialized shouldBe true
        }

        "Test that an applied event can alter aggregate state"{
            data class SomeState(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            class SomeInitEvent : Event<SomeState>

            class SomeLaterEvent : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init { someInitEvent: SomeInitEvent, aggregateId: UUID ->
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
                                    aggregateId = UUID.randomUUID(),
                                    event = SomeInitEvent(),
                                    eventNumber = -1,
                                    serializationId = SomeInitEvent::class.simpleName!!
                            ),
                            currentState = null)

            val updatedState = aggregateConfig
                    .getConfiguration { it.simpleName!! }
                    .applyEvent(
                            EventWrapper(
                                    aggregateId = UUID.randomUUID(),
                                    event = SomeLaterEvent(),
                                    eventNumber = -1,
                                    serializationId = SomeLaterEvent::class.simpleName!!
                            ),
                            initializedState
                    )

            updatedState!!.stateUpdated shouldBe true
        }

        "Test that an aggregate can have multiple initializers"{
            data class SomeState(val initializedWith: String) : Aggregate

            class SomeInitEvent : Event<SomeState>

            class SomeOtherInitEvent : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init { someInitEvent: SomeInitEvent, aggregateId: UUID ->
                        SomeState(
                                initializedWith = "SomeInitEvent"
                        )
                    }

                    init { someOtherInitEvent: SomeOtherInitEvent, aggregateId: UUID ->
                        SomeState(
                                initializedWith = "SomeOtherInitEvent"
                        )
                    }
                }
            }

            val initializedState0 = aggregateConfig
                    .getConfiguration { it.simpleName!! }
                    .applyEvent(EventWrapper(UUID.randomUUID(),SomeInitEvent(),null, -1, SomeInitEvent::class.simpleName!!), null)
            initializedState0!!.initializedWith shouldBe "SomeInitEvent"

            val initializedState1 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeOtherInitEvent(),null, -1, SomeOtherInitEvent::class.simpleName!!), null)
            initializedState1!!.initializedWith shouldBe "SomeOtherInitEvent"

        }

        "Test that an aggregate can have the same event as an initializer and applicator, and that the correct one is invoked depending on if the aggregate exists or not"{
            data class SomeState(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            class SomeInitEvent : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init { someInitEvent: SomeInitEvent, aggregateId: UUID ->
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

            val initializedState = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeInitEvent(),null, -1, SomeInitEvent::class.simpleName!!), null)
            initializedState!!.stateInitialized shouldBe true
            val updatedState = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeInitEvent(),null, -1, SomeInitEvent::class.simpleName!!), initializedState)
            updatedState!!.stateUpdated shouldBe true
        }

        "Test that an exception is thrown if an \"apply\" event is received by an uninitialized aggregate" {
            data class SomeState(val stateUpdated: Boolean = false) : Aggregate

            class SomeEvent : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    apply<SomeEvent> {
                        copy(
                                stateUpdated = true
                        )
                    }
                }
            }

            shouldThrow<IllegalStateException> { aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeEvent(),null, Random.nextLong(), SomeEvent::class.simpleName!!), null)}
                    .message shouldContain "aggregate state has not yet been initialized"

        }

        "Test that subsequent initializers are ignored if the aggregate is already initialized"{
            data class SomeState(val initializedWith: String) : Aggregate

            class SomeInitEvent : Event<SomeState>

            class SomeOtherInitEvent : Event<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init { someInitEvent: SomeInitEvent, aggregateId: UUID ->
                        SomeState(
                                initializedWith = "SomeInitEvent"
                        )
                    }

                    init { someOtherInitEvent: SomeOtherInitEvent, aggregateId: UUID ->
                        SomeState(
                                initializedWith = "SomeOtherInitEvent"
                        )
                    }
                }
            }

            val derivedState0 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeInitEvent(),null, -1, SomeInitEvent::class.simpleName!!), null)
            derivedState0!!.initializedWith shouldBe "SomeInitEvent"

            val derivedState1 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeOtherInitEvent(),null, -1, SomeOtherInitEvent::class.simpleName!!), derivedState0)
            derivedState1!!.initializedWith shouldBe "SomeInitEvent"

        }
    }
}