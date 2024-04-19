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
                                Event(
                                    aggregateId = UUID.randomUUID(),
                                    eventData = SomeInitEventData()
                                ),
                                    eventNumber = -1,
                                    serializationId = SomeInitEventData::class.simpleName!!
                            ),
                            currentState = null
                    )

            initializedState!!.stateInitialized shouldBe true
        }

        "Test that an applied eventdata can alter aggregate state"{
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
                                Event(
                                    aggregateId = UUID.randomUUID(),
                                    eventData = SomeInitEventData())
                                ,
                                    eventNumber = -1,
                                    serializationId = SomeInitEventData::class.simpleName!!
                            ),
                            currentState = null)

            val updatedState = aggregateConfig
                    .getConfiguration { it.simpleName!! }
                    .applyEvent(
                            EventWrapper(
                                Event(
                                    aggregateId = UUID.randomUUID(),
                                    eventData = SomeLaterEventData()
                                ),
                                    eventNumber = -1,
                                    serializationId = SomeLaterEventData::class.simpleName!!
                            ),
                            initializedState
                    )

            updatedState!!.stateUpdated shouldBe true
        }

        "Test that an applied event can alter aggregate state"{
            data class SomeState(val stateInitialized: Boolean, val stateUpdated: Boolean = false, val someText: String) : Aggregate

            class SomeInitEventData(val initText: String) : EventData<SomeState>

            class SomeLaterEventData(val updateText: String) : EventData<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    initEvent { event: Event<SomeInitEventData>, _: UUID ->
                        SomeState(
                            stateInitialized = true,
                            someText = event.eventData.initText
                        )
                    }

                    applyEvent<SomeLaterEventData> {
                        copy(
                            stateUpdated = true,
                            someText = it.eventData.updateText
                        )
                    }
                }
            }
            val aggregateId = UUID.randomUUID()

            val initializedState = aggregateConfig
                .getConfiguration { it.simpleName!! }
                .applyEvent(
                    wrapper = EventWrapper(
                        Event(
                            aggregateId = aggregateId,
                            eventData = SomeInitEventData("init"))
                        ,
                        eventNumber = -1,
                        serializationId = SomeInitEventData::class.simpleName!!
                    ),
                    currentState = null)
            initializedState!!.someText shouldBe "init"

            val updatedState = aggregateConfig
                .getConfiguration { it.simpleName!! }
                .applyEvent(
                    EventWrapper(
                        Event(
                            aggregateId = aggregateId,
                            eventData = SomeLaterEventData("update")
                        ),
                        eventNumber = -1,
                        serializationId = SomeLaterEventData::class.simpleName!!
                    ),
                    initializedState
                )

            updatedState!!.stateUpdated shouldBe true
            updatedState.someText shouldBe "update"

        }

        "Test that an applied eventwrapper can alter aggregate state"{
            data class SomeState(val stateInitialized: Boolean, val stateUpdated: Boolean = false, val someText: String) : Aggregate

            class SomeInitEventData(val initText: String) : EventData<SomeState>

            class SomeLaterEventData(val updateText: String) : EventData<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    initWrapper { eventWrapper: EventWrapper<SomeInitEventData>, _: UUID ->
                        SomeState(
                            stateInitialized = true,
                            someText = eventWrapper.event.eventData.initText
                        )
                    }

                    applyWrapper<SomeLaterEventData> {
                        copy(
                            stateUpdated = true,
                            someText = it.event.eventData.updateText
                        )
                    }
                }
            }
            val aggregateId = UUID.randomUUID()

            val initializedState = aggregateConfig
                .getConfiguration { it.simpleName!! }
                .applyEvent(
                    wrapper = EventWrapper(
                        Event(
                            aggregateId = aggregateId,
                            eventData = SomeInitEventData("init"))
                        ,
                        eventNumber = -1,
                        serializationId = SomeInitEventData::class.simpleName!!
                    ),
                    currentState = null)
            initializedState!!.someText shouldBe "init"

            val updatedState = aggregateConfig
                .getConfiguration { it.simpleName!! }
                .applyEvent(
                    EventWrapper(
                        Event(
                            aggregateId = aggregateId,
                            eventData = SomeLaterEventData("update")
                        ),
                        eventNumber = -1,
                        serializationId = SomeLaterEventData::class.simpleName!!
                    ),
                    initializedState
                )

            updatedState!!.stateUpdated shouldBe true
            updatedState.someText shouldBe "update"

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
                    .applyEvent(EventWrapper(Event(UUID.randomUUID(),SomeInitEventData(),null), -1, SomeInitEventData::class.simpleName!!), null)
            initializedState0!!.initializedWith shouldBe "SomeInitEvent"

            val initializedState1 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(Event(UUID.randomUUID(),SomeOtherInitEventData(),null), -1, SomeOtherInitEventData::class.simpleName!!), null)
            initializedState1!!.initializedWith shouldBe "SomeOtherInitEvent"

        }

        "Test that an aggregate can have multiple initializers with event"{
            data class SomeState(val initializedWith: String) : Aggregate

            class SomeInitEventData : EventData<SomeState>

            class SomeOtherInitEventData : EventData<SomeState>

            val aggregateConfig = object : AggregateConfiguration<SomeState>("employee") {
                init {
                    initEvent { _: Event<SomeInitEventData>, _: UUID ->
                        SomeState(
                            initializedWith = "SomeInitEvent"
                        )
                    }

                    initEvent { _: Event<SomeOtherInitEventData>, _: UUID ->
                        SomeState(
                            initializedWith = "SomeOtherInitEvent"
                        )
                    }
                }
            }

            val initializedState0 = aggregateConfig
                .getConfiguration { it.simpleName!! }
                .applyEvent(EventWrapper(Event(UUID.randomUUID(),SomeInitEventData(),null), -1, SomeInitEventData::class.simpleName!!), null)
            initializedState0!!.initializedWith shouldBe "SomeInitEvent"

            val initializedState1 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(Event(UUID.randomUUID(),SomeOtherInitEventData(),null), -1, SomeOtherInitEventData::class.simpleName!!), null)
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

            val initializedState = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(Event(UUID.randomUUID(),SomeInitEventData(),null), -1, SomeInitEventData::class.simpleName!!), null)
            initializedState!!.stateInitialized shouldBe true
            val updatedState = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(Event(UUID.randomUUID(),SomeInitEventData(),null), -1, SomeInitEventData::class.simpleName!!), initializedState)
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

            shouldThrow<IllegalStateException> { aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(Event(UUID.randomUUID(),SomeEventData(),null), Random.nextLong(), SomeEventData::class.simpleName!!), null)}
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

            val derivedState0 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(Event(UUID.randomUUID(),SomeInitEventData(),null), -1, SomeInitEventData::class.simpleName!!), null)
            derivedState0!!.initializedWith shouldBe "SomeInitEvent"

            val derivedState1 = aggregateConfig.getConfiguration { it.simpleName!! }.applyEvent(EventWrapper(Event(UUID.randomUUID(),SomeOtherInitEventData(),null), -1, SomeOtherInitEventData::class.simpleName!!), derivedState0)
            derivedState1!!.initializedWith shouldBe "SomeInitEvent"

        }
    }
}