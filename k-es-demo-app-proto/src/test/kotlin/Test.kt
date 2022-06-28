import com.github.msemys.esjc.EventStoreBuilder
import com.google.protobuf.Message
import io.kotest.assertions.timing.eventually
import io.kotest.core.listeners.TestListener
import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.extensions.testcontainers.perSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import mu.KotlinLogging
import no.ks.kes.demoapp.Konto
import no.ks.kes.demoapp.KontoAggregate
import no.ks.kes.demoapp.KontoCmds
import no.ks.kes.esjc.EsjcAggregateRepository
import no.ks.kes.esjc.EsjcEventSubscriberFactory
import no.ks.kes.esjc.EsjcEventUtil
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

private const val PORT = 1113

@ExperimentalTime
class Test : StringSpec() {

    val dockerImageName = DockerImageName.parse("eventstore/eventstore:21.10.1-buster-slim")
    val eventStoreContainer = GenericContainer<GenericContainer<*>>(dockerImageName)
        .withCommand("--insecure", "--run-projections=All")
        .withExposedPorts(PORT)
        .waitingFor(Wait.forLogMessage(".*initialized.*\\n", 4));

    init {
        val klient = EventStoreTestKlientListener(portProvider = { eventStoreContainer.getMappedPort(PORT)})
        listeners(eventStoreContainer.perSpec(), klient)
        "Test at vi kan opprette konto" {
            val validatedAggregateConfiguration = Konto.getConfiguration { klient.repo.getSerializationId(it) }

            val kontoId = UUID.randomUUID()
            val orgId = UUID.randomUUID().toString()
            log.info { "AggregateId $kontoId, OrgId $orgId" }

            val receivedEvents = AtomicInteger(0)
            val receivedCatchupEvents = AtomicInteger(0)

            klient.subscriberFactory.createSubscriber("subscriber", -1, {
                log.info { "Got event: ${it.eventNumber} - ${it.event.aggregateId} - ${it.event.eventData}" }
                receivedEvents.incrementAndGet()
            })

            klient.kontoCmds.handle(KontoCmds.Opprett(kontoId, orgId))
            klient.kontoCmds.handle(KontoCmds.Aktiver(kontoId))

            var aggregateResult = klient.repo.read(kontoId, validatedAggregateConfiguration)

            aggregateResult.shouldBeInstanceOf<AggregateReadResult.InitializedAggregate<KontoAggregate>>()
            aggregateResult.aggregateState.aktivert shouldBe true

            klient.kontoCmds.handle(KontoCmds.Deaktiver(kontoId))

            aggregateResult = klient.repo.read(kontoId, validatedAggregateConfiguration)
            aggregateResult.shouldBeInstanceOf<AggregateReadResult.InitializedAggregate<KontoAggregate>>()
            aggregateResult.aggregateState.aktivert shouldBe false

            aggregateResult.aggregateState.aggregateId shouldBe kontoId

            val subscriber = klient.subscriberFactory.createSubscriber("catchup subscriber", 1, {
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

internal class EventStoreTestKlientListener(private val portProvider: () -> Int): TestListener {
    lateinit var kontoCmds: KontoCmds
    lateinit var repo: AggregateRepository
    lateinit var subscriberFactory: EsjcEventSubscriberFactory
    override suspend fun beforeSpec(spec: Spec) {
        val eventStore = EventStoreBuilder.newBuilder()
            .singleNodeAddress("localhost", portProvider.invoke())
            .userCredentials("admin", "changeit")
            .build()

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

        repo = EsjcAggregateRepository(eventStore, eventSerdes, EsjcEventUtil.defaultStreamName("no.ks.kes.proto.demo"),jacksonEventMetadataSerdes)

        kontoCmds = KontoCmds(repo)

        subscriberFactory = EsjcEventSubscriberFactory(eventStore, eventSerdes, "no.ks.kes.proto.demo")
    }
}