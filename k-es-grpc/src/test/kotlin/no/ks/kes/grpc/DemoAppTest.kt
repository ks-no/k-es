package no.ks.kes.grpc

import com.eventstore.dbclient.Endpoint
import com.eventstore.dbclient.EventStoreDBClient
import com.eventstore.dbclient.EventStoreDBClientSettings
import com.google.protobuf.Message
import io.kotest.assertions.timing.eventually
import io.kotest.core.annotation.EnabledIf
import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import mu.KotlinLogging
import no.ks.kes.demoapp.Konto
import no.ks.kes.demoapp.KontoAggregate
import no.ks.kes.demoapp.KontoCmds
import no.ks.kes.lib.AggregateReadResult
import no.ks.kes.lib.AggregateRepository
import no.ks.kes.serdes.jackson.JacksonEventMetadataSerdes
import no.ks.kes.serdes.proto.ProtoEventData
import no.ks.kes.serdes.proto.ProtoEventDeserializer
import no.ks.kes.serdes.proto.ProtoEventSerdes
import no.ks.svarut.event.Avsender
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.toDuration


private val log = KotlinLogging.logger {}

@ExperimentalTime
@EnabledIf(enabledIf = DisableOnArm64::class)
class DemoAppTest : StringSpec() {

    lateinit var kontoCmds: KontoCmds
    lateinit var repo: AggregateRepository
    lateinit var subscriberFactory: GrpcEventSubscriberFactory

    val dockerImageName = DockerImageName.parse("eventstore/eventstore:21.6.0-buster-slim")
    val eventStoreContainer = GenericContainer<GenericContainer<*>>(dockerImageName)
        .withEnv("EVENTSTORE_RUN_PROJECTIONS","All")
        .withEnv("EVENTSTORE_START_STANDARD_PROJECTIONS","True")
        .withEnv("EVENTSTORE_CLUSTER_SIZE","1")
        .withEnv("EVENTSTORE_INSECURE", "True")
        .withEnv("EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP", "True")
        .withEnv("EVENTSTORE_ENABLE_EXTERNAL_TCP", "True")
        .withEnv("EVENTSTORE_LOG_LEVEL", "Verbose")
        .withExposedPorts(1113, 2113)
        .withReuse(true)
        .waitingFor(Wait.forLogMessage(".*initialized.*\\n", 4))

    override suspend fun beforeSpec(spec: Spec) {
        eventStoreContainer.start()

        val eventStoreClient = EventStoreDBClient.create(EventStoreDBClientSettings.builder()
            .addHost(Endpoint("localhost", eventStoreContainer.getMappedPort(2113)))
            .defaultCredentials("admin", "changeit").tls(false).dnsDiscover(false)
            .buildConnectionSettings())

        val eventSerdes = ProtoEventSerdes(
            mapOf(
                Konto.AvsenderOpprettet::class to Avsender.AvsenderOpprettet.getDefaultInstance(),
                Konto.AvsenderAktivert::class to Avsender.AvsenderAktivert.getDefaultInstance(),
                Konto.AvsenderDeaktivert::class to Avsender.AvsenderDeaktivert.getDefaultInstance(),
            ),
            object: ProtoEventDeserializer {
                override fun deserialize(msg: Message): ProtoEventData<*> {
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

        repo = GrpcAggregateRepository(eventStoreClient, eventSerdes, GrpcEventUtil.defaultStreamName("no.ks.kes.proto.demo"),jacksonEventMetadataSerdes)

        subscriberFactory = GrpcEventSubscriberFactory(eventStoreClient, eventSerdes, "no.ks.kes.proto.demo")

        kontoCmds = KontoCmds(repo)
    }

    override suspend fun afterSpec(spec: Spec) {
        eventStoreContainer.stop()
    }

    init {
        "Test at vi kan opprette konto" {

            val validatedAggregateConfiguration = Konto.getConfiguration { repo.getSerializationId(it) }

            val receivedEvents = AtomicInteger(0)
            val receivedCatchupEvents = AtomicInteger(0)

            val kontoId = UUID.randomUUID()
            val orgId = UUID.randomUUID().toString()
            log.info { "AggregateId $kontoId, OrgId $orgId" }

            subscriberFactory.createSubscriber("subscriber", -1, {
                receivedEvents.incrementAndGet()
            })

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

            val subscriber = subscriberFactory.createSubscriber("catchup subscriber", 1, {
                    receivedCatchupEvents.incrementAndGet()
            })

            eventually(5.toDuration(DurationUnit.SECONDS)) {
                receivedEvents.get() shouldBe 3
                receivedCatchupEvents.get() shouldBe 1
                subscriber.lastProcessedEvent() shouldBe 2
            }
        }
    }
}