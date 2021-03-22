import com.github.msemys.esjc.EventStoreBuilder
import com.google.protobuf.Message
import io.kotest.matchers.types.shouldBeInstanceOf
import mu.KotlinLogging
import no.ks.kes.demoapp.*
import no.ks.kes.esjc.EsjcAggregateRepository
import no.ks.kes.esjc.EsjcEventUtil
import no.ks.kes.lib.*
import no.ks.kes.serdes.proto.ProtoEventDeserializer
import no.ks.kes.serdes.proto.ProtoEvent
import no.ks.kes.serdes.proto.ProtoEventSerdes
import no.ks.svarut.event.Avsender
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.util.*


private val log = KotlinLogging.logger {}

class Test {

    companion object {

        lateinit var kontoCmds: KontoCmds
        lateinit var repo: AggregateRepository

        val dockerImageName = DockerImageName.parse("eventstore/eventstore:release-5.0.6")
        val eventStoreContainer = GenericContainer<GenericContainer<*>>(dockerImageName)
            .withEnv("EVENTSTORE_RUN_PROJECTIONS","All")
            .withEnv("EVENTSTORE_START_STANDARD_PROJECTIONS","True")
            .withExposedPorts(1113)
            .waitingFor(Wait.forLogMessage(".*initialized.*\\n", 4));

        @BeforeAll
        @JvmStatic
        fun beforeClass() {
            eventStoreContainer.start()

            val eventStore = EventStoreBuilder.newBuilder()
                .singleNodeAddress("localhost", eventStoreContainer.getMappedPort(1113))
                .userCredentials("admin", "changeit")
                .build()

            val eventSerdes = ProtoEventSerdes(
                mapOf(
                    Konto.AvsenderOpprettet::class to Avsender.AvsenderOpprettet.getDefaultInstance(),
                    Konto.AvsenderAktivert::class to Avsender.AvsenderAktivert.getDefaultInstance(),
                    Konto.AvsenderDeaktivert::class to Avsender.AvsenderDeaktivert.getDefaultInstance(),
                ),
                object: ProtoEventDeserializer {
                    override fun deserialize(msg: Message): ProtoEvent<*> {
                        return when (msg) {
                            is Avsender.AvsenderOpprettet -> Konto.AvsenderOpprettet(msg = msg)
                            is Avsender.AvsenderAktivert -> Konto.AvsenderAktivert(msg = msg)
                            is Avsender.AvsenderDeaktivert -> Konto.AvsenderDeaktivert(msg = msg)
                            else -> throw RuntimeException("Event ${msg::class.java} mangler konvertering")
                        }
                    }

                }
            )

            val jacksonEventMetadataSerdes = JacksonEventMetadataSerdes(Konto.DemoMetadata::class)

            repo = EsjcAggregateRepository(eventStore, eventSerdes, EsjcEventUtil.defaultStreamName("no.ks.kes.proto.demo"),jacksonEventMetadataSerdes)

            kontoCmds = KontoCmds(repo)
        }

        @AfterAll
        @JvmStatic
        fun afterClass() {
            eventStoreContainer.stop()
        }
    }

    @Test
    @DisplayName("Test at vi kan opprette konto")
    internal fun testOpprettKonto() {
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