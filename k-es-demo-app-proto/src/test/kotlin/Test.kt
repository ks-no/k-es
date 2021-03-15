import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import mu.KotlinLogging
import no.ks.kes.demoapp.*
import no.ks.kes.lib.AggregateReadResult
import no.ks.kes.lib.AggregateRepository
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.util.*


private val log = KotlinLogging.logger {}

@SpringBootTest(classes = [Application::class])
class Test {


/*    companion object {

        val dockerImageName = DockerImageName.parse("eventstore/eventstore:release-5.0.6")
        val eventStoreContainer = GenericContainer<GenericContainer<*>>(dockerImageName)
            .withEnv("EVENTSTORE_RUN_PROJECTIONS","All")
            .withEnv("EVENTSTORE_START_STANDARD_PROJECTIONS","True")
            .withExposedPorts(1113, 2113)
            .waitingFor(Wait.forLogMessage(".*initialized.*\\n", 4));

        @BeforeAll
        @JvmStatic
        fun beforeClass() {
            log.info ("Start ***********************")
            eventStoreContainer.start()
        }

        @AfterAll
        @JvmStatic
        fun afterClass() {
            log.info ("END *******************")
            eventStoreContainer.stop()
        }
    }*/
    @Test
    @DisplayName("Test at vi kan opprette konto")
    internal fun testOpprettKonto(@Autowired kontoCmds: KontoCmds, @Autowired repo: AggregateRepository) {
        val validatedAggregateConfiguration = Konto.getConfiguration { repo.getSerializationId(it) }

        val kontoId = UUID.randomUUID()
        val orgId = UUID.randomUUID().toString()
        log.info { "AggregateId $kontoId, OrgId $orgId" }

        kontoCmds.handle(KontoCmds.Opprett(kontoId, orgId))
        kontoCmds.handle(KontoCmds.Aktiver(kontoId))

        var aggregateResult = repo.read(kontoId, validatedAggregateConfiguration)

        aggregateResult.shouldBeInstanceOf<AggregateReadResult.InitializedAggregate<KontoAggregate>>()
        aggregateResult.aggregateState.aktivert shouldBe true

        kontoCmds.handle(KontoCmds.Deaktiver(kontoId))

        aggregateResult = repo.read(kontoId, validatedAggregateConfiguration)
        aggregateResult.shouldBeInstanceOf<AggregateReadResult.InitializedAggregate<KontoAggregate>>()
        aggregateResult.aggregateState.aktivert shouldBe false

        aggregateResult.aggregateState.aggregateId shouldBe kontoId
    }
}