package no.ks.kes.demoapp

import com.eventstore.dbclient.EventStoreDBClient
import com.eventstore.dbclient.EventStoreDBClientSettings
import mu.KotlinLogging
import no.ks.kes.grpc.GrpcAggregateRepository
import no.ks.kes.grpc.GrpcEventUtil
import no.ks.kes.grpc.GrpcEventSubscriberFactory
import no.ks.kes.jdbc.projection.SqlServerProjectionRepository
import no.ks.kes.jdbc.saga.SqlServerCommandQueue
import no.ks.kes.jdbc.saga.SqlServerSagaRepository
import no.ks.kes.lib.*
import no.ks.kes.serdes.jackson.JacksonCmdSerdes
import no.ks.kes.serdes.jackson.JacksonEventSerdes
import no.ks.kes.serdes.jackson.JacksonSagaStateSerdes
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.jdbc.DataSourceBuilder
import org.springframework.context.ApplicationListener
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}

@Configuration
@SpringBootApplication
class Application {

    @Bean
    fun datasource(@Value("\${mssql.host}") host: String, @Value("\${mssql.port}") port: String): DataSource =
            DataSourceBuilder.create().apply {
                driverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                url("jdbc:sqlserver://$host:$port;databaseName=kesdemo;encrypt=false")
                username("SA")
                password("Test1234!")
            }.build()

    @Bean
    fun eventStore(@Value("\${eventstore.host}") host: String, @Value("\${eventstore.port}") port: String): EventStoreDBClient =  EventStoreDBClient.create(
        EventStoreDBClientSettings.builder()
            .addHost(host, port.toInt())
            .defaultCredentials("admin", "changeit").tls(false)
            .buildConnectionSettings())

    @Bean
    fun eventSerdes(): EventSerdes = JacksonEventSerdes(setOf(
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
    fun aggregateRepo(eventStoreDBClient: EventStoreDBClient, eventSerdes: EventSerdes): AggregateRepository =
            GrpcAggregateRepository(eventStoreDBClient, eventSerdes, GrpcEventUtil.defaultStreamName("no.ks.kes.demoapp"))

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
    fun subscriber(eventStoreDBClient: EventStoreDBClient, eventSerdes: EventSerdes): EventSubscriberFactory<*> =
            GrpcEventSubscriberFactory(eventStoreDBClient, eventSerdes, "no.ks.kes.demoapp")

    @Component
    class MyInitializer(
            val shipments: Shipments,
            val eventSubscriberFactory: EventSubscriberFactory<*>,
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
                    hwmId = "ProjectionManager",
                    onError = {
                        log.error(it) { "Event subscription for Projections was closed." }
                    }
            )

            Sagas.initialize(
                    eventSubscriberFactory = eventSubscriberFactory,
                    sagaRepository = SqlServerSagaRepository(
                            dataSource = dataSource,
                            sagaStateSerdes = JacksonSagaStateSerdes(),
                            cmdSerdes = cmdSerdes),
                    sagas = setOf(ShipmentSaga),
                    commandQueue = SqlServerCommandQueue(dataSource, cmdSerdes, setOf(basketCmds, shipmentCmds)),
                    pollInterval = 500,
                onError = {
                        log.error(it) { "Event subscription for Sagas was closed." }
                    }
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