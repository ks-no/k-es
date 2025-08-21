package no.ks.kes.grpc

import com.eventstore.dbclient.EventStoreDBClient
import com.eventstore.dbclient.EventStoreDBClientSettings
import com.google.protobuf.Message
import io.kotest.assertions.asClue
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.listeners.AfterSpecListener
import io.kotest.core.listeners.BeforeSpecListener
import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.nulls.beNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNot
import io.kotest.matchers.types.shouldBeInstanceOf
import mu.KotlinLogging
import no.ks.kes.demoapp.Konto
import no.ks.kes.demoapp.KontoAggregate
import no.ks.kes.demoapp.KontoCmds
import no.ks.kes.lib.AggregateReadResult
import no.ks.kes.serdes.jackson.JacksonEventMetadataSerdes
import no.ks.kes.serdes.proto.ProtoEventData
import no.ks.kes.serdes.proto.ProtoEventDeserializer
import no.ks.kes.serdes.proto.ProtoEventSerdes
import no.ks.svarut.event.Avsender
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.toDuration


private val log = KotlinLogging.logger {}
private val logConsumer = Slf4jLogConsumer(log).withSeparateOutputStreams().withPrefix("eventstore")
private const val STREAM_NAME = "no.ks.kes.proto.demo"

@ExperimentalTime
class DemoAppTest : StringSpec(), BeforeSpecListener, AfterSpecListener {

    private val dockerImageName = CompletableFuture.supplyAsync {
        val imageName = "eventstore/eventstore"
        when(System.getProperty("os.arch")) {
            "aarch64" -> "$imageName:21.10.9-alpha-arm64v8"
            else -> "$imageName:21.6.0-buster-slim"
        }
    }
    private val eventStoreContainer = GenericContainer(dockerImageName)
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
        .withLogConsumer(logConsumer)

    private val eventSerdes = ProtoEventSerdes(
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

    private val jacksonEventMetadataSerdes = JacksonEventMetadataSerdes(Konto.DemoMetadata::class)

    override suspend fun beforeSpec(spec: Spec) {
        eventStoreContainer.start()
    }

    override suspend fun afterSpec(spec: Spec) {
        eventStoreContainer.stop()
    }

    init {
        "Test at vi kan opprette konto" {
            val eventStoreClient: EventStoreDBClient by lazy {
                EventStoreDBClient.create(EventStoreDBClientSettings.builder()
                    .addHost(eventStoreContainer.host, eventStoreContainer.getMappedPort(2113))
                    .defaultCredentials("admin", "changeit").tls(false).dnsDiscover(false)
                    .buildConnectionSettings().also { settings ->
                        log.info { "Connection hosts: ${settings.hosts.joinToString { "${it.hostName}:${it.port}" }} $settings" }
                    })
            }
            val repo = GrpcAggregateRepository(eventStoreClient, eventSerdes, GrpcEventUtil.defaultStreamName(STREAM_NAME), jacksonEventMetadataSerdes)

            val validatedAggregateConfiguration = Konto.getConfiguration { eventSerdes.getSerializationId(it) }

            val receivedEvents = AtomicInteger(0)
            val receivedCatchupEvents = AtomicInteger(0)

            val kontoId = UUID.randomUUID()
            val orgId = UUID.randomUUID().toString()
            log.info { "AggregateId $kontoId, OrgId $orgId" }
            val subscriberFactory: GrpcEventSubscriberFactory by lazy {
                GrpcEventSubscriberFactory(eventStoreClient, eventSerdes, STREAM_NAME)
            }
            val subscription = subscriberFactory.createSubscriber("subscriber", -1, {
                receivedEvents.incrementAndGet()
            }) {}

            subscription.asClue {
                it.isSubscribedToAll shouldBe false
                it.subscriptionId() shouldNot beNull()
            }

            repo.read(kontoId, validatedAggregateConfiguration).asClue {
                it.shouldBeInstanceOf<AggregateReadResult.NonExistingAggregate>()
            }
            val kontoCmds = KontoCmds(repo)
            kontoCmds.handle(KontoCmds.Opprett(kontoId, orgId))

            repo.read(kontoId, validatedAggregateConfiguration).asClue {
                it.shouldBeInstanceOf<AggregateReadResult.InitializedAggregate<KontoAggregate>>()
                it.aggregateState.aktivert shouldBe false
                it.eventNumber shouldBe 0
            }

            kontoCmds.handle(KontoCmds.Aktiver(kontoId))
            repo.read(kontoId, validatedAggregateConfiguration).asClue {
                it.shouldBeInstanceOf<AggregateReadResult.InitializedAggregate<KontoAggregate>>()
                it.aggregateState.aktivert shouldBe true
                it.eventNumber shouldBe 1
            }

            kontoCmds.handle(KontoCmds.Deaktiver(kontoId))
            repo.read(kontoId, validatedAggregateConfiguration).asClue {
                it.shouldBeInstanceOf<AggregateReadResult.InitializedAggregate<KontoAggregate>>()
                it.aggregateState.aktivert shouldBe false
                it.eventNumber shouldBe 2
                it.aggregateState.aggregateId shouldBe kontoId
            }

            val subscriber = subscriberFactory.createSubscriber("catchup subscriber", 1, {
                    receivedCatchupEvents.incrementAndGet()
            }){}

            eventually(5.toDuration(DurationUnit.SECONDS)) {
                receivedEvents.get() shouldBe 3
                receivedCatchupEvents.get() shouldBe 1
                subscriber.lastProcessedEvent() shouldBe 2
            }
        }
    }
}