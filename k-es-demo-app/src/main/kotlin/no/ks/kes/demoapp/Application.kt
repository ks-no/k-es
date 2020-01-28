package no.ks.kes.demoapp

import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.EventStoreBuilder
import no.ks.kes.esjc.EsjcAggregateRepository
import no.ks.kes.esjc.EsjcEventSubscriber
import no.ks.kes.esjc.EsjcEventUtil
import no.ks.kes.lib.*
import no.ks.kes.sagajdbc.SqlServerCommandQueueManager
import no.ks.kes.sagajdbc.SqlServerSagaRepository
import no.ks.kes.sagajdbc.SqlServerSagaTimeoutManager
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
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource


fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}

@SpringBootApplication
@EnableScheduling
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
    fun eventSerdes(): EventSerdes<String> = JacksonEventSerdes(setOf(
            Basket.Created::class,
            Basket.ItemAdded::class,
            Basket.CheckedOut::class,
            Shipment.Created::class,
            Shipment.Failed::class
    ))

    @Bean
    fun cmdSerdes(): CmdSerdes<String> = JacksonCmdSerdes(setOf(
            BasketCmds.Create::class,
            BasketCmds.AddItem::class,
            BasketCmds.CheckOut::class,
            ShipmentCmds.Request::class
    ))

    @Bean
    fun shippedBaskets(): Shipments = Shipments()

    @Bean
    fun basketCmd(aggregateRepository: AggregateRepository): BasketCmds =
            BasketCmds(aggregateRepository, object : PaymentProcessor {
                override fun process(orderId: UUID) {}
            })

    @Bean
    fun shipmentCmd(aggregateRepository: AggregateRepository, warehouseManager: WarehouseManager): ShipmentCmds =
            ShipmentCmds(aggregateRepository, warehouseManager)

    @Bean
    fun subscriber(eventStore: EventStore, eventSerdes: EventSerdes<String>): EventSubscriber =
            EsjcEventSubscriber(eventStore, eventSerdes, "no.ks.kes.demoapp")

    @Bean
    @DependsOn("flyway", "flywayInitializer")
    fun aggregateRepo(eventStore: EventStore, eventSerdes: EventSerdes<String>): AggregateRepository =
            EsjcAggregateRepository(eventStore, eventSerdes, EsjcEventUtil.defaultStreamName("no.ks.kes.demoapp"))

    @Bean
    fun eventStore(): EventStore = EventStoreBuilder.newBuilder()
            .singleNodeAddress("localhost", 1113)
            .userCredentials("admin", "changeit")
            .build()

    @Bean
    @DependsOn("flyway", "flywayInitializer")
    fun sqlServerCmdQueueManager(dataSource: DataSource, cmdSerdes: CmdSerdes<String>, basketCmds: BasketCmds, shipmentCmds: ShipmentCmds): SqlServerCommandQueueManager {
        return SqlServerCommandQueueManager(dataSource, cmdSerdes, setOf(basketCmds, shipmentCmds))
    }

    @Bean
    @DependsOn("flyway", "flywayInitializer")
    fun sagaManager(dataSource: DataSource,
                    cmdSerdes: CmdSerdes<String>,
                    eventSubscriber: EventSubscriber): SagaManager {
        return SagaManager(eventSubscriber, SqlServerSagaRepository(dataSource, JacksonSagaStateSerdes(), cmdSerdes), setOf(CreateShipmentSaga()))
    }

    @Bean
    @DependsOn("flyway", "flywayInitializer")
    fun sqlServerSagaTimeoutManager(dataSource: DataSource, sagaManager: SagaManager): SqlServerSagaTimeoutManager {
        return SqlServerSagaTimeoutManager(dataSource, sagaManager)
    }

    @Bean
    fun warehouseManager(): WarehouseManager = MyWarehouseManager()

    @Component
    class MyBootListener(
            val shippedBaskets: Shipments,
            val eventSubscriber: EventSubscriber
    ) : ApplicationListener<ApplicationReadyEvent> {
        override fun onApplicationEvent(applicationReadyEvent: ApplicationReadyEvent) {
            ProjectionManager(eventSubscriber, setOf(shippedBaskets), 0, {}, {})
        }
    }

    @Component
    class QueuePoller(val cmdQueueManager: SqlServerCommandQueueManager) {

        @Scheduled(fixedDelay = 1000)
        fun poll() {
            cmdQueueManager.poll()
        }
    }

    @Component
    class TimeoutPoller(val timeoutManager: SqlServerSagaTimeoutManager) {

        @Scheduled(fixedDelay = 1000)
        fun poll() {
            timeoutManager.poll()
        }
    }

    class MyWarehouseManager : WarehouseManager {
        private var fail: AtomicReference<Exception?> = AtomicReference(null)

        override fun failOnce(e: Exception?) {
            fail.set(e)
        }

        override fun shipOrder(orderId: UUID) {
            fail.getAndSet(null)?.let { throw it }
        }
    }
}