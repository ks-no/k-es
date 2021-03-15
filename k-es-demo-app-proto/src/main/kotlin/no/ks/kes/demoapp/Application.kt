package no.ks.kes.demoapp

import com.github.msemys.esjc.EventStore
import com.github.msemys.esjc.EventStoreBuilder
import no.ks.kes.esjc.EsjcAggregateRepository
import no.ks.kes.esjc.EsjcEventSubscriberFactory
import no.ks.kes.esjc.EsjcEventUtil
import no.ks.kes.lib.*
import no.ks.kes.serdes.proto.ProtoEventSerdes
import no.ks.svarut.event.Avsender
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean

fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}

@SpringBootApplication
class Application {

    @Bean
    fun eventStore(): EventStore = EventStoreBuilder.newBuilder()
            .singleNodeAddress("localhost", 1113)
            .userCredentials("admin", "changeit")
            .build()

    @Bean
    fun eventSerdes(): EventSerdes = ProtoEventSerdes(
        mapOf(
            Konto.AvsenderOpprettet::class to Avsender.AvsenderOpprettet.getDefaultInstance(),
            Konto.AvsenderAktivert::class to Avsender.AvsenderAktivert.getDefaultInstance(),
            Konto.AvsenderDeaktivert::class to Avsender.AvsenderDeaktivert.getDefaultInstance(),
        ))

    @Bean
    fun aggregateRepo(eventStore: EventStore, eventSerdes: EventSerdes): AggregateRepository =
            EsjcAggregateRepository(eventStore, eventSerdes, EsjcEventUtil.defaultStreamName("no.ks.kes.proto.demo"))

    @Bean
    fun basketCmd(aggregateRepository: AggregateRepository): KontoCmds = KontoCmds(aggregateRepository)

    @Bean
    fun subscriber(eventStore: EventStore, eventSerdes: EventSerdes) = EsjcEventSubscriberFactory(eventStore, eventSerdes, "no.ks.kes.proto.demo")

}