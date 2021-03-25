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

            class SomeInitEventData : EventData<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init { _: SomeInitEventData, _: UUID ->
                        SomeState(stateInitialized = true)
                    }
                }
            }

            val initializedState = aggregateConfig
                    .getConfiguration { it.simpleName!! }
                    .applyEvent(
                            wrapper = EventWrapper(
                                    aggregateId = UUID.randomUUID(),
                                    event = SomeInitEventData(),
                                    eventNumber = -1,
                                    serializationId = SomeInitEventData::class.simpleName!!
                            ),
                            currentState = null
                    )

            initializedState!!.stateInitialized shouldBe true
        }

        "Test that an applied event can alter aggregate state"{
            data class SomeState(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            class SomeInitEventData : EventData<SomeState>

            class SomeLaterEventData : EventData<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init { _: SomeInitEventData, _: UUID ->
                        SomeState(
                                stateInitialized = true
                        )
                    }

                    apply<SomeLaterEventData> {
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
                                    event = SomeInitEventData(),
                                    eventNumber = -1,
                                    serializationId = SomeInitEventData::class.simpleName!!
                            ),
                            currentState = null)

            val updatedState = aggregateConfig
                    .getConfiguration { it.simpleName!! }
                    .applyEvent(
                            EventWrapper(
                                    aggregateId = UUID.randomUUID(),
                                    event = SomeLaterEventData(),
                                    eventNumber = -1,
                                    serializationId = SomeLaterEventData::class.simpleName!!
                            ),
                            initializedState
                    )

            updatedState!!.stateUpdated shouldBe true
        }

        "Test that an aggregate can have multiple initializers"{
            data class SomeState(val initializedWith: String) : Aggregate

            class SomeInitEventData : EventData<SomeState>

            class SomeOtherInitEventData : EventData<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init { _: SomeInitEventData, _: UUID ->
                        SomeState(
                                initializedWith = "SomeInitEvent"
                        )
                    }

                    init { _: SomeOtherInitEventData, _: UUID ->
                        SomeState(
                                initializedWith = "SomeOtherInitEvent"
                        )
                    }
                }
            }

            val initializedState0 = aggregateConfig
                    .getConfiguration { it.simpleName!! }
                    .applyEvent(EventWrapper(UUID.randomUUID(),SomeInitEventData(),null, -1, SomeInitEventData::class.simpleName!!), null)
            initializedState0!!.initializedWith shouldBe "SomeInitEvent"

            val initializedState1 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeOtherInitEventData(),null, -1, SomeOtherInitEventData::class.simpleName!!), null)
            initializedState1!!.initializedWith shouldBe "SomeOtherInitEvent"

        }

        "Test that an aggregate can have the same event as an initializer and applicator, and that the correct one is invoked depending on if the aggregate exists or not"{
            data class SomeState(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

            class SomeInitEventData : EventData<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init { _: SomeInitEventData, _: UUID ->
                        SomeState(
                                stateInitialized = true
                        )
                    }

                    apply<SomeInitEventData> {
                        copy(
                                stateUpdated = true
                        )
                    }
                }
            }

            val initializedState = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeInitEventData(),null, -1, SomeInitEventData::class.simpleName!!), null)
            initializedState!!.stateInitialized shouldBe true
            val updatedState = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeInitEventData(),null, -1, SomeInitEventData::class.simpleName!!), initializedState)
            updatedState!!.stateUpdated shouldBe true
        }

        "Test that an exception is thrown if an \"apply\" event is received by an uninitialized aggregate" {
            data class SomeState(val stateUpdated: Boolean = false) : Aggregate

            class SomeEventData : EventData<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    apply<SomeEventData> {
                        copy(
                                stateUpdated = true
                        )
                    }
                }
            }

            shouldThrow<IllegalStateException> { aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeEventData(),null, Random.nextLong(), SomeEventData::class.simpleName!!), null)}
                    .message shouldContain "aggregate state has not yet been initialized"

        }

        "Test that subsequent initializers are ignored if the aggregate is already initialized"{
            data class SomeState(val initializedWith: String) : Aggregate

            class SomeInitEventData : EventData<SomeState>

            class SomeOtherInitEventData : EventData<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    init { _: SomeInitEventData, _: UUID ->
                        SomeState(
                                initializedWith = "SomeInitEvent"
                        )
                    }

                    init { _: SomeOtherInitEventData, _: UUID ->
                        SomeState(
                                initializedWith = "SomeOtherInitEvent"
                        )
                    }
                }
            }

            val derivedState0 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeInitEventData(),null, -1, SomeInitEventData::class.simpleName!!), null)
            derivedState0!!.initializedWith shouldBe "SomeInitEvent"

            val derivedState1 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(UUID.randomUUID(),SomeOtherInitEventData(),null, -1, SomeOtherInitEventData::class.simpleName!!), derivedState0)
            derivedState1!!.initializedWith shouldBe "SomeInitEvent"

        }
    }
}