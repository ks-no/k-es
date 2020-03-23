package no.ks.kes.demoapp

import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.EventStoreBuilder
import no.ks.kes.esjc.EsjcAggregateRepository
import no.ks.kes.esjc.EsjcEventSubscriberFactory
import no.ks.kes.esjc.EsjcEventUtil
import no.ks.kes.jdbc.projection.SqlServerProjectionRepository
import no.ks.kes.jdbc.saga.SqlServerCommandQueue
import no.ks.kes.jdbc.saga.SqlServerSagaRepository
import no.ks.kes.lib.*
import no.ks.kes.serdes.jackson.JacksonCmdSerdes
import no.ks.kes.serdes.jackson.JacksonEventSerdes
import no.ks.kes.serdes.jackson.JacksonSagaStateSerdes
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.jdbc.DataSourceBuilder
import org.springframework.context.ApplicationListener
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource


fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}

@SpringBootApplication
class Application {

    @Bean
    fun datasource(): DataSource =
            DataSourceBuilder.create().apply {
                driverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                url("jdbc:sqlserver://localhost:1433;databaseName=kesdemo")
                username("SA")
                password("Test1234!")
            }.build()

    @Bean
    fun eventStore(): EventStore = EventStoreBuilder.newBuilder()
            .singleNodeAddress("localhost", 1113)
            .userCredentials("admin", "changeit")
            .build()

    @Bean
    fun eventSerdes(): EventSerdes<String> = JacksonEventSerdes(setOf(
            Basket.Created::class,
            Basket.ItemAdded::class,
            Basket.CheckedOut::class,
            Shipment.Prepared::class,
            Shipment.Failed::class,
            Shipment.WarehouseNotifiedOfMissingShipment::class
    ))

    @Bean
    fun shippedBaskets(): Shipments = Shipments()

    @Bean
    @DependsOn("flyway", "flywayInitializer")
    fun aggregateRepo(eventStore: EventStore, eventSerdes: EventSerdes<String>): AggregateRepository =
            EsjcAggregateRepository(eventStore, eventSerdes, EsjcEventUtil.defaultStreamName("no.ks.kes.demoapp"))

    @Bean
    fun basketCmd(aggregateRepository: AggregateRepository): BasketCmds =
            BasketCmds(aggregateRepository, object : PaymentProcessor {
                override fun process(orderId: UUID) {}
            })

    @Bean
    fun warehouseManager(): WarehouseManager = MyWarehouseManager()

    @Bean
    fun shipmentCmd(aggregateRepository: AggregateRepository, warehouseManager: WarehouseManager): ShipmentCmds =
            ShipmentCmds(aggregateRepository, warehouseManager)

    @Bean
    fun subscriber(eventStore: EventStore, eventSerdes: EventSerdes<String>): EventSubscriberFactory =
            EsjcEventSubscriberFactory(eventStore, eventSerdes, "no.ks.kes.demoapp")

    @Component
    class MyInitializer(
            val shipments: Shipments,
            val eventSubscriberFactory: EventSubscriberFactory,
            val dataSource: DataSource,
            val basketCmds: BasketCmds,
            val shipmentCmds: ShipmentCmds
    ) : ApplicationListener<ApplicationReadyEvent> {
        override fun onApplicationEvent(applicationReadyEvent: ApplicationReadyEvent) {
            val cmdSerdes = JacksonCmdSerdes(setOf(
                    BasketCmds.Create::class,
                    BasketCmds.AddItem::class,
                    BasketCmds.CheckOut::class,
                    ShipmentCmds.Request::class,
                    ShipmentCmds.SendMissingShipmentAlert::class))

            Projections.initialize(
                    eventSubscriberFactory = eventSubscriberFactory,
                    projections = setOf(shipments),
                    projectionRepository = SqlServerProjectionRepository(dataSource),
                    subscriber = "ProjectionManager"
            )

            Sagas.initialize(
                    eventSubscriberFactory = eventSubscriberFactory,
                    sagaRepository = SqlServerSagaRepository(
                            dataSource = dataSource,
                            sagaStateSerdes = JacksonSagaStateSerdes(),
                            cmdSerdes = cmdSerdes),
                    sagas = setOf(CreateShipmentSaga()),
                    commandQueue = SqlServerCommandQueue(dataSource, cmdSerdes, setOf(basketCmds, shipmentCmds)),
                    pollInterval = 500
            )
        }
    }

    class MyWarehouseManager : WarehouseManager {
        private var fail: AtomicReference<Exception?> = AtomicReference(null)

        override fun failOnce(e: Exception?) {
            fail.set(e)
        }

        override fun investigateMissingShipment(orderId: UUID) {}

        override fun shipOrder(orderId: UUID) {
            fail.getAndSet(null)?.let { throw it }
        }
    }
}