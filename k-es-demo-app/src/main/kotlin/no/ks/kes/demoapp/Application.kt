package no.ks.kes.demoapp

import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.EventStoreBuilder
import no.ks.kes.esjc.EsjcAggregateRepository
import no.ks.kes.esjc.EsjcEventSubscriber
import no.ks.kes.lib.AggregateRepository
import no.ks.kes.lib.EventSubscriber
import no.ks.kes.lib.ProjectionManager
import no.ks.kes.lib.SagaManager
import no.ks.kes.sagajdbc.JdbcSagaRepository
import no.ks.kes.sagajdbc.SqlServerCommandQueueManager
import no.ks.kes.serdes.jackson.JacksonCmdSerdes
import no.ks.kes.serdes.jackson.JacksonEventSerdes
import no.ks.kes.serdes.jackson.JacksonSagaStateSerdes
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import java.util.*
import javax.annotation.PostConstruct
import javax.sql.DataSource

fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}

@SpringBootApplication
class Application {
    val sagaSerdes =  JacksonSagaStateSerdes()
    val eventSerdes =  JacksonEventSerdes(setOf(SessionStarted::class, ItemAddedToBasket::class, BasketCheckedOut::class))
    val cmdSerdes = JacksonCmdSerdes(setOf(BasketCmds.StartSession::class, BasketCmds.AddItemToBasket::class, BasketCmds.CheckOutBasket::class))

    @Bean
    fun mostPopularItem():MostPopularItem{
        return MostPopularItem()
    }

    @Bean
    fun basketCmd(aggregateRepository: AggregateRepository): BasketCmds{
        return BasketCmds(aggregateRepository, object : PaymentProcessor {
            override fun process(orderId: UUID) {
                TODO("not implemented")
            }
        })
    }

    @Bean
    fun shipmentCmd(aggregateRepository: AggregateRepository): ShipmentCmds{
        return ShipmentCmds(aggregateRepository, object : WarehouseManager {
            override fun shipOrder(orderId: UUID) {
                TODO("not implemented")
            }
        })
    }

    @Bean
    fun subscriber(eventStore: EventStore): EventSubscriber{
        return EsjcEventSubscriber(eventStore, eventSerdes,0L, "no.ks.kes.demoapp")
    }

    @Bean
    fun aggregateRepo(eventStore: EventStore): AggregateRepository{
        return EsjcAggregateRepository(eventStore, eventSerdes) { t, id -> "somestream"}
    }

    @Bean
    fun eventStore(): EventStore {
        EventStoreBuilder.newBuilder()
                .singleNodeAddress("localhost", 1353)
                .userCredentials("admin", "changeit")
    }

    @PostConstruct
    fun sagaManager(dataSource: DataSource, mostPopularItem: MostPopularItem, basketCmds: BasketCmds, shipmentCmds: ShipmentCmds, eventSubscriber: EventSubscriber){
        SagaManager(eventSubscriber, JdbcSagaRepository(dataSource, sagaSerdes, cmdSerdes), setOf(CreateOrderSaga()))
        ProjectionManager(eventSubscriber, setOf(mostPopularItem), {}, {})
        SqlServerCommandQueueManager(dataSource, cmdSerdes, setOf(basketCmds, shipmentCmds))
    }
}