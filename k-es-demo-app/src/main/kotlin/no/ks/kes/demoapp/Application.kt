package no.ks.kes.demoapp

import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.EventStoreBuilder
import no.ks.kes.esjc.EsjcAggregateRepository
import no.ks.kes.esjc.EsjcEventSubscriber
import no.ks.kes.lib.*
import no.ks.kes.sagajdbc.JdbcSagaRepository
import no.ks.kes.sagajdbc.SqlServerCommandQueueManager
import no.ks.kes.serdes.jackson.JacksonCmdSerdes
import no.ks.kes.serdes.jackson.JacksonEventSerdes
import no.ks.kes.serdes.jackson.JacksonSagaStateSerdes
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.jdbc.DataSourceBuilder
import org.springframework.context.ApplicationListener
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component
import java.util.*
import javax.sql.DataSource


fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}

@SpringBootApplication
class Application {

    @Bean
    fun datasource():DataSource {
            val dataSourceBuilder = DataSourceBuilder.create();
            dataSourceBuilder.driverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            dataSourceBuilder.url("spring.datasource.url=jdbc:sqlserver://localhost:1433;databaseName=kes-demo");
            dataSourceBuilder.username("SA");
            dataSourceBuilder.password("Test1234!");
            return dataSourceBuilder.build();
    }

    @Bean
    fun eventSerdes(): EventSerdes<String> = JacksonEventSerdes(setOf(SessionStarted::class, ItemAddedToBasket::class, BasketCheckedOut::class))

    @Bean
    fun cmdSerdes(): CmdSerdes<String> = JacksonCmdSerdes(setOf(BasketCmds.StartSession::class, BasketCmds.AddItemToBasket::class, BasketCmds.CheckOutBasket::class))

    @Bean
    fun sagaSerdes(): SagaStateSerdes<String> = JacksonSagaStateSerdes()

    @Bean
    fun mostPopularItem(): MostPopularItem = MostPopularItem()

    @Bean
    fun basketCmd(aggregateRepository: AggregateRepository): BasketCmds =
            BasketCmds(aggregateRepository, object : PaymentProcessor {
                override fun process(orderId: UUID) {
                    TODO("not implemented")
                }
            })

    @Bean
    fun shipmentCmd(aggregateRepository: AggregateRepository): ShipmentCmds =
            ShipmentCmds(aggregateRepository, object : WarehouseManager {
                override fun shipOrder(orderId: UUID) {
                    TODO("not implemented")
                }
            })

    @Bean
    fun subscriber(eventStore: EventStore, eventSerdes: EventSerdes<String>): EventSubscriber =
            EsjcEventSubscriber(eventStore, eventSerdes, 0L, "no.ks.kes.demoapp")

    @Bean
    fun aggregateRepo(eventStore: EventStore, eventSerdes: EventSerdes<String>): AggregateRepository =
            EsjcAggregateRepository(eventStore, eventSerdes) { t, id -> "somestream" }

    @Bean
    fun eventStore(): EventStore = EventStoreBuilder.newBuilder()
            .singleNodeAddress("localhost", 1113)
            .userCredentials("admin", "changeit")
            .build()

    @Component
    class MyBootListener(
            val dataSource: DataSource,
            val mostPopularItem: MostPopularItem,
            val basketCmds: BasketCmds,
            val shipmentCmds: ShipmentCmds,
            val sagaSerdes: SagaStateSerdes<String>,
            val cmdSerdes: CmdSerdes<String>,
            val eventSubscriber: EventSubscriber
    ) : ApplicationListener<ApplicationReadyEvent> {

        override fun onApplicationEvent(applicationReadyEvent: ApplicationReadyEvent) {
            SagaManager(eventSubscriber, JdbcSagaRepository(dataSource, sagaSerdes, cmdSerdes), setOf(CreateOrderSaga()))
            ProjectionManager(eventSubscriber, setOf(mostPopularItem), {}, {})
            SqlServerCommandQueueManager(dataSource, cmdSerdes, setOf(basketCmds, shipmentCmds))
        }
    }
}