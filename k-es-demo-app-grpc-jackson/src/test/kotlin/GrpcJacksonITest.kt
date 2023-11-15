import io.kotest.assertions.asClue
import io.kotest.matchers.types.shouldBeSameInstanceAs
import no.ks.kes.demoapp.*
import no.ks.kes.lib.AggregateReadResult
import no.ks.kes.lib.AggregateRepository
import org.awaitility.kotlin.await
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.*


@Testcontainers
@SpringBootTest(classes = [Application::class])
@DisabledIfSystemProperty(named = "os.arch", matches = "aarch64", disabledReason = "Ikke støttet på arm arkitektur")
class GrpcJacksonITest {

    companion object {

        val eventStoreDockerImageName = DockerImageName.parse("eventstore/eventstore:21.6.0-buster-slim")
        val mssqlDockerImageName = DockerImageName.parse("docker-all.artifactory.fiks.ks.no/fiks-mssql")

        @JvmStatic
        @Container
        val eventStoreContainer = GenericContainer(eventStoreDockerImageName)
            .withEnv("EVENTSTORE_RUN_PROJECTIONS","All")
            .withEnv("EVENTSTORE_START_STANDARD_PROJECTIONS","True")
            .withEnv("EVENTSTORE_CLUSTER_SIZE","1")
            .withEnv("EVENTSTORE_INSECURE", "True")
            .withEnv("EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP", "True")
            .withEnv("EVENTSTORE_ENABLE_EXTERNAL_TCP", "True")
            .withExposedPorts(1113, 2113)
            .waitingFor(Wait.forLogMessage(".*initialized.*\\n", 4))

        @JvmStatic
        @Container
        val mssqlContainer = GenericContainer(mssqlDockerImageName)
            .withEnv("ACCEPT_EULA", "Y")
            .withEnv("SA_PASSWORD", "Test1234!")
            .withEnv("DB_NAME", "kesdemo")
            .withExposedPorts(1433)
            .waitingFor(Wait.forLogMessage(".*Starting up database 'kesdemo'.*\\n", 1))

        @JvmStatic
        @DynamicPropertySource
        fun registerEventstoreProperties(registry: DynamicPropertyRegistry) {
            registry.add("eventstore.host") { eventStoreContainer.host }
            registry.add("eventstore.port") { eventStoreContainer.getMappedPort(2113) }
            registry.add("mssql.host") { mssqlContainer.host }
            registry.add("mssql.port") { mssqlContainer.getMappedPort(1433) }
        }

    }

    @Test
    @DisplayName("Test that we can checkout a basket, and that this creates a shipment")
    internal fun testCreateShipment(@Autowired basketCmds: BasketCmds, @Autowired shippedBaskets: Shipments) {
        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))

        await untilCallTo { shippedBaskets.getShipments(basketId) } matches { it!!.contains(itemId) }
    }

    @Test
    @DisplayName("Test that a shipment fails permanently if the items are no longer carried")
    internal fun testCreateShipmentFails(@Autowired basketCmds: BasketCmds, @Autowired shipments: Shipments, @Autowired warehouseManager: WarehouseManager) {
        warehouseManager.failOnce(ItemNoLongerCarried())

        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))

        await untilCallTo { shipments.isFailedShipment(basketId) } matches { it == true }
    }

    @Test
    @DisplayName("Test that a shipment is retried if the warehouse system fails")
    internal fun testCreateShipmentRetry(@Autowired basketCmds: BasketCmds, @Autowired shipments: Shipments, @Autowired warehouseManager: WarehouseManager) {
        warehouseManager.failOnce(WarehouseSystemFailure())

        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))

        await untilCallTo { shipments.getShipments(basketId) } matches { it!!.contains(itemId)}
    }

    @Test
    @DisplayName("Test that adding the same item to a basket multiple times creates a shipment with multiple copies of the item")
    internal fun testCreateShipmentMultipleItems(@Autowired basketCmds: BasketCmds, @Autowired shippedBaskets: Shipments) {
        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))

        await untilCallTo { shippedBaskets.getShipments(basketId)?.get(itemId) } matches {it == 2}
    }

    @Test
    @DisplayName("Test that adding an item to a closed basket fails")
    internal fun testAddItemToClosedBasket(@Autowired basketCmds: BasketCmds, @Autowired shippedBaskets: Shipments) {
        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))
        assertThrows<IllegalStateException> {basketCmds.handle(BasketCmds.AddItem(basketId, itemId))}
    }

    @Test
    @DisplayName("Test that checking out a empty basket fails")
    internal fun testCheckOutClosedBasket(@Autowired basketCmds: BasketCmds, @Autowired shippedBaskets: Shipments) {
        val basketId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        assertThrows<IllegalStateException> {basketCmds.handle(BasketCmds.CheckOut(basketId))}
    }

    @Test
    @DisplayName("Test that a shipment which isnt confirmed within two seconds is marked as missing")
    internal fun testShipmentMissing(@Autowired basketCmds: BasketCmds, @Autowired shipments: Shipments) {
        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))

        await untilCallTo { shipments.isMissingShipment(basketId) } matches { it == true }
    }

    @Test
    @DisplayName("Test that we can create a basket, add 1000 items to it, and check it out")
    internal fun testAdd1000Items(@Autowired basketCmds: BasketCmds, @Autowired shipments: Shipments) {
        val basketId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        repeat(1000) {
            basketCmds.handle(BasketCmds.AddItem(basketId, UUID.randomUUID()))
        }
        basketCmds.handle(BasketCmds.CheckOut(basketId))

        await untilCallTo { shipments.getShipments(basketId) } matches { it?.size == 1000 }
    }

    @Test
    @DisplayName("Test that reading from a stream without events returns a NonExistingAggregate")
    internal fun testReadAggregateNonExisting(@Autowired basketCmds: BasketCmds, @Autowired aggregateRepository: AggregateRepository) {
        val basketId = UUID.randomUUID()

        aggregateRepository.read(basketId, Basket.getConfiguration { aggregateRepository.getSerializationId(it) }).asClue {
            it shouldBeSameInstanceAs AggregateReadResult.NonExistingAggregate
        }
    }
}