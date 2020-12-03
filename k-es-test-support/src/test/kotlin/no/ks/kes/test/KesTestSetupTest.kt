package no.ks.kes.test

import io.kotest.assertions.timing.eventually
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeSameInstanceAs
import no.ks.kes.lib.Projections
import no.ks.kes.serdes.jackson.JacksonCmdSerdes
import no.ks.kes.serdes.jackson.JacksonEventSerdes
import no.ks.kes.test.example.Cmds
import no.ks.kes.test.example.EngineCmdHandler
import no.ks.kes.test.example.EnginesProjection
import no.ks.kes.test.example.Events
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

@ExperimentalTime
class KesTestSetupTest : FunSpec({

    test("Creates subscriberFactory") {
        KesTestSetup(eventSerdes = JacksonEventSerdes(emptySet()), cmdSerdes = JacksonCmdSerdes(emptySet())).use {
            kesTestSetup -> kesTestSetup.subscriberFactory shouldNotBe null
        }
    }

    test("Creates aggregateRepository") {
        KesTestSetup(eventSerdes = JacksonEventSerdes(emptySet()), cmdSerdes = JacksonCmdSerdes(emptySet())).use {
            kesTestSetup -> kesTestSetup.aggregateRepository shouldNotBe null
        }
    }

    test("Exposes eventSerdes") {
        val eventSerdes = JacksonEventSerdes(emptySet())
        KesTestSetup(eventSerdes = eventSerdes, cmdSerdes = JacksonCmdSerdes(emptySet())).use {
            kesTestSetup -> kesTestSetup.eventSerdes shouldBeSameInstanceAs eventSerdes
        }
    }

    test("Exposes cmdSerdes") {
        val cmdSerdes = JacksonCmdSerdes(emptySet())
        KesTestSetup(eventSerdes = JacksonEventSerdes(emptySet()), cmdSerdes = cmdSerdes).use {
            kesTestSetup -> kesTestSetup.cmdSerdes shouldBeSameInstanceAs cmdSerdes
        }
    }

    test("Projections using test framework") {
        val enginesProjection = EnginesProjection()
        KesTestSetup(eventSerdes = Events.serdes, cmdSerdes = Cmds.serdes).use {
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
        KesTestSetup(eventSerdes = Events.serdes, cmdSerdes = Cmds.serdes).use {

        }
    }
})
