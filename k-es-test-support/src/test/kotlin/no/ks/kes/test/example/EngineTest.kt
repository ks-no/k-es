package no.ks.kes.test.example

import io.kotest.assertions.asClue
import io.kotest.assertions.fail
import io.kotest.assertions.failure
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.assertions.throwables.shouldThrowExactly
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.beEmpty
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldHaveAtLeastSize
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import io.kotest.property.Arb
import io.kotest.property.arbitrary.UUIDVersion
import io.kotest.property.arbitrary.uuid
import io.kotest.property.checkAll
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import no.ks.kes.lib.Projections
import no.ks.kes.lib.Sagas
import no.ks.kes.test.AggregateKey
import no.ks.kes.test.withKes
import java.util.*
import java.util.concurrent.Executors
import kotlin.time.*

@ExperimentalTime
class EngineTest : StringSpec({

    "Test command handler" {
        withKes(eventSerdes = Events.serdes, cmdSerdes = Cmds.serdes) { kes ->
            val engineCmdHandler = EngineCmdHandler(kes.aggregateRepository)
            val aggregateId = UUID.randomUUID()
            engineCmdHandler.handle(Cmds.Create(aggregateId)).asClue {
                it.id shouldBe aggregateId
                it.running shouldBe false
                it.startCount shouldBe 0
            }
            eventually(3.toDuration(DurationUnit.SECONDS)) {
                kes.eventStream.get(AggregateKey(ENGINE_AGGREGATE_TYPE, aggregateId))?.asClue { events ->
                    events shouldHaveSize 1
                    events[0].eventData::class.java shouldBe Events.Created::class.java
                } ?: fail("No events was found for aggregate")
            }
        }
    }

    "Test command handler using several threads" {
        withKes(eventSerdes = Events.serdes, cmdSerdes = Cmds.serdes) { kes ->
            val engineCmdHandler = EngineCmdHandler(kes.aggregateRepository)
            val aggregateId = UUID.randomUUID()
            engineCmdHandler.handle(Cmds.Create(aggregateId)).asClue {
                it.id shouldBe aggregateId
                it.running shouldBe false
                it.startCount shouldBe 0
            }
            eventually(3.toDuration(DurationUnit.SECONDS)) {
                kes.eventStream.get(AggregateKey(ENGINE_AGGREGATE_TYPE, aggregateId))?.asClue { events ->
                    events shouldHaveSize 1
                    events[0].eventData::class.java shouldBe Events.Created::class.java
                } ?: fail("No events was found for aggregate")
            }

            Executors.newFixedThreadPool(10).asCoroutineDispatcher().use { dispatcher ->
                awaitAll(
                        async(dispatcher) { engineCmdHandler.handleUnsynchronized(Cmds.Start(aggregateId)) },
                        async(dispatcher) { engineCmdHandler.handleUnsynchronized(Cmds.Start(aggregateId)) },
                        async(dispatcher) { engineCmdHandler.handleUnsynchronized(Cmds.Start(aggregateId)) },
                        async(dispatcher) { engineCmdHandler.handleUnsynchronized(Cmds.Start(aggregateId)) },
                        async(dispatcher) { engineCmdHandler.handleUnsynchronized(Cmds.Start(aggregateId)) },
                        async(dispatcher) { engineCmdHandler.handleUnsynchronized(Cmds.Start(aggregateId)) },
                        async(dispatcher) { engineCmdHandler.handleUnsynchronized(Cmds.Start(aggregateId)) },
                        async(dispatcher) { engineCmdHandler.handleUnsynchronized(Cmds.Start(aggregateId)) },
                        async(dispatcher) { engineCmdHandler.handleUnsynchronized(Cmds.Start(aggregateId)) },
                        async(dispatcher) { engineCmdHandler.handleUnsynchronized(Cmds.Start(aggregateId)) }
                )

            }
            eventually(3.toDuration(DurationUnit.SECONDS)) {
                kes.eventStream.get(AggregateKey(ENGINE_AGGREGATE_TYPE, aggregateId))?.asClue { writeEventWrappers ->
                    writeEventWrappers shouldHaveAtLeastSize 2
                    val events = writeEventWrappers.map { it.eventData }.toList()
                    events.filterIsInstance<Events.Created>() shouldHaveSize 1
                    // At this point we really don't know how many of these events was applied as EngineCmdHandler checks aggregate state before generating Started events
                    // As we are using the handleUnsynchronized function we can therefore not guarantee how many started events are generated
                    events.filterIsInstance<Events.Started>() shouldHaveAtLeastSize 1
                } ?: fail("No events was found for aggregate")
            }
        }
    }

    "Test command handler by providing event and command types explicitly" {
        withKes(events = Events.all, cmds = Cmds.all) { kes ->
            val engineCmdHandler = EngineCmdHandler(kes.aggregateRepository)
            val aggregateId = UUID.randomUUID()
            engineCmdHandler.handle(Cmds.Create(aggregateId)).asClue {
                it.id shouldBe aggregateId
                it.running shouldBe false
                it.startCount shouldBe 0
            }
            eventually(3.toDuration(DurationUnit.SECONDS)) {
                kes.eventStream.get(AggregateKey(ENGINE_AGGREGATE_TYPE, aggregateId))?.asClue { events ->
                    events shouldHaveSize 1
                    events[0].eventData::class.java shouldBe Events.Created::class.java
                } ?: fail("No events was found for aggregate")
            }
        }
    }

    "Test saga with timeout" {
        withKes(Events.serdes, Cmds.serdes) { kes ->
            val engineCmdHandler = EngineCmdHandler(kes.aggregateRepository)
            val commandQueue = kes.createCommandQueue(setOf(engineCmdHandler))
            Sagas.initialize(
                    eventSubscriberFactory = kes.subscriberFactory,
                    sagaRepository = kes.createSagaRepository(commandQueue),
                    sagas = setOf(EngineSaga),
                    commandQueue = commandQueue,
                    pollInterval = 1000L
            ) {
                e -> failure("Failed to handle saga event", e)
            }
            val aggregateId = UUID.randomUUID()
            engineCmdHandler.handle(Cmds.Create(aggregateId)).asClue {
                it.id shouldBe aggregateId
                it.running shouldBe false
                it.startCount shouldBe 0
            }
            eventually(10.toDuration(DurationUnit.SECONDS)) {
                kes.eventStream.get(AggregateKey(ENGINE_AGGREGATE_TYPE, aggregateId))?.asClue { wrappers ->
                    val events = wrappers.map { it.eventData }.toList()
                    events.filterIsInstance<Events.Created>() shouldHaveSize 1
                    events.filterIsInstance<Events.Started>() shouldHaveSize 1
                    events.filterIsInstance<Events.Stopped>() shouldHaveSize 1
                } ?: fail("No events was found for aggregate")
                engineCmdHandler.handle(Cmds.Check(aggregateId)).asClue {
                    it.running shouldBe false
                    it.startCount shouldBe 1
                }
            }
        }
    }

    "Test using both projection and saga" {
        withKes(Events.serdes, Cmds.serdes) { kes ->
            val engineCmdHandler = EngineCmdHandler(kes.aggregateRepository)
            val commandQueue = kes.createCommandQueue(setOf(engineCmdHandler))
            val engineProjection = EnginesProjection()
            Sagas.initialize(
                    eventSubscriberFactory = kes.subscriberFactory,
                    sagaRepository = kes.createSagaRepository(commandQueue),
                    sagas = setOf(EngineSaga),
                    commandQueue = commandQueue,
                    pollInterval = 1000L
            ) {
                e -> failure("Failed to handle saga event", e)
            }
            Projections.initialize(eventSubscriberFactory = kes.subscriberFactory,
                    projections = setOf(engineProjection),
                    projectionRepository = kes.projectionRepository,
                    hwmId = testCase.name.testName
            ) { e ->
                failure("Failed during eventhandling in projection", e)
            }
            val aggregatesCreated = 10
            checkAll(iterations = aggregatesCreated, Arb.uuid(UUIDVersion.V4, false)) { aggregateId ->
                engineCmdHandler.handle(Cmds.Create(aggregateId)).asClue {
                    it.id shouldBe aggregateId
                    it.running shouldBe false
                    it.startCount shouldBe 0
                }
            }

            eventually(30.toDuration(DurationUnit.SECONDS)) {
                engineProjection.all shouldHaveSize aggregatesCreated
                engineProjection.allRunning should beEmpty()
                engineProjection.allStopped shouldHaveSize aggregatesCreated
            }
        }
    }

    "Test projection" {
        withKes(Events.serdes, Cmds.serdes) { kes ->
            val engineCmdHandler = EngineCmdHandler(kes.aggregateRepository)
            val engineProjection = EnginesProjection()
            Projections.initialize(
                    eventSubscriberFactory = kes.subscriberFactory,
                    projections = setOf(engineProjection),
                    projectionRepository = kes.projectionRepository,
                    hwmId = testCase.name.testName
            ) { e ->
                failure("Failed during projection event handling", e)
            }
            val aggregateId = UUID.randomUUID()
            engineCmdHandler.handle(Cmds.Create(aggregateId)).asClue {
                it.id shouldBe aggregateId
                it.running shouldBe false
                it.startCount shouldBe 0
            }
            eventually(3.toDuration(DurationUnit.SECONDS)) {
                engineProjection.all shouldContain aggregateId
            }

        }

    }

    "Test projection when you have 1000 Created events" {
        withKes(Events.serdes, Cmds.serdes) { kes ->
            val engineCmdHandler = EngineCmdHandler(kes.aggregateRepository)
            val engineProjection = EnginesProjection()
            Projections.initialize(
                    eventSubscriberFactory = kes.subscriberFactory,
                    projections = setOf(engineProjection),
                    projectionRepository = kes.projectionRepository,
                    hwmId = testCase.name.testName
            ) { e ->
                failure("Failed during projection event handling", e)
            }
            val aggregatesToCreate = 1000
            checkAll(aggregatesToCreate, Arb.uuid(UUIDVersion.V4, false)) { aggregateId ->
                engineCmdHandler.handle(Cmds.Create(aggregateId)).asClue {
                    it.id shouldBe aggregateId
                    it.running shouldBe false
                    it.startCount shouldBe 0
                }
            }
            eventually(3.toDuration(DurationUnit.SECONDS)) {
                engineProjection.all shouldHaveSize aggregatesToCreate
            }

        }
    }


    "Test issuing commands that are not init commands before the aggregate has been created" {
        withKes(Events.serdes, Cmds.serdes) { kes ->
            val engineCmdHandler = EngineCmdHandler(kes.aggregateRepository)
            checkAll(iterations = 100, Arb.uuid(UUIDVersion.V4, false)) { aggregateId ->
                shouldThrowExactly<IllegalStateException> {
                    // The Start command is not declared a init command in EngineCmdHandler and thus this command should rejected as the aggregate does not exist
                    engineCmdHandler.handle(Cmds.Start(aggregateId))
                } shouldHaveMessage "Aggregate $aggregateId does not exist, and cmd Start is not configured as an initializer. Consider adding an \"init\" configuration for this command."
            }
        }
    }

})
