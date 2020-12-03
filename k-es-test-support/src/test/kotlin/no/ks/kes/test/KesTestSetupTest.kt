package no.ks.kes.test

import io.kotest.assertions.asClue
import io.kotest.assertions.fail
import io.kotest.assertions.failure
import io.kotest.assertions.timing.eventually
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeSameInstanceAs
import no.ks.kes.lib.Projections
import no.ks.kes.lib.Sagas
import no.ks.kes.serdes.jackson.JacksonCmdSerdes
import no.ks.kes.serdes.jackson.JacksonEventSerdes
import no.ks.kes.test.example.*
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

@ExperimentalTime
class KesTestSetupTest : FunSpec({

    test("Creates subscriberFactory") {
        withKes(eventSerdes = JacksonEventSerdes(emptySet()), cmdSerdes = JacksonCmdSerdes(emptySet())) {
            kesTestSetup -> kesTestSetup.subscriberFactory shouldNotBe null
        }
    }

    test("Creates aggregateRepository") {
        withKes(eventSerdes = JacksonEventSerdes(emptySet()), cmdSerdes = JacksonCmdSerdes(emptySet())) {
            kesTestSetup -> kesTestSetup.aggregateRepository shouldNotBe null
        }
    }

    test("Exposes eventSerdes") {
        val eventSerdes = JacksonEventSerdes(emptySet())
        withKes(eventSerdes = eventSerdes, cmdSerdes = JacksonCmdSerdes(emptySet())) {
            kesTestSetup -> kesTestSetup.eventSerdes shouldBeSameInstanceAs eventSerdes
        }
    }

    test("Exposes cmdSerdes") {
        val cmdSerdes = JacksonCmdSerdes(emptySet())
        withKes(eventSerdes = JacksonEventSerdes(emptySet()), cmdSerdes = cmdSerdes) {
            kesTestSetup -> kesTestSetup.cmdSerdes shouldBeSameInstanceAs cmdSerdes
        }
    }

    test("Projections using test framework") {
        val enginesProjection = EnginesProjection()
        withKes(eventSerdes = Events.serdes, cmdSerdes = Cmds.serdes) {
            Projections.initialize(
                    eventSubscriberFactory = it.subscriberFactory,
                    subscriber = testCase.displayName,
                    projectionRepository = it.projectionRepository,
                    projections = setOf(enginesProjection)
            )
            val cmdHandler = EngineCmdHandler(repository = it.aggregateRepository)
            val aggregateId = UUID.randomUUID()
            cmdHandler.handle(Cmds.Create(aggregateId))
            eventually(5.seconds) {
                enginesProjection.all shouldContain aggregateId
            }
        }
    }

    test("Sagas using test framework") {
        withKes(eventSerdes = Events.serdes, cmdSerdes = Cmds.serdes) { kes ->
            val cmdHandler = EngineCmdHandler(repository = kes.aggregateRepository)
            val commandQueue = kes.createCommandQueue(setOf(cmdHandler))
            val sagaRepository = kes.createSagaRepository(commandQueue)
            Sagas.initialize(eventSubscriberFactory = kes.subscriberFactory,
                    sagaRepository = sagaRepository,
                    sagas = setOf(EngineSaga),
                    commandQueue = commandQueue,
                    pollInterval = 10
            ) {
                e -> failure("Failed to process event for saga", e)
            }
            val aggregateId = UUID.randomUUID()
            cmdHandler.handle(Cmds.Create(aggregateId)).asClue {
                it.id shouldBe aggregateId
            }
            eventually(10.seconds) {
                sagaRepository.getSagaState(aggregateId, SAGA_SERILIZATION_ID, EngineSagaState::class)?.asClue {
                    it.stoppedBySaga shouldBe true
                } ?: fail("EngineSaga did not change state of aggregate to be stopped")
            }

        }
    }
})
