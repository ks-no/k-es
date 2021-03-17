package no.ks.kes.demoapp

import mu.KotlinLogging
import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.AggregateConfiguration
import no.ks.kes.lib.EventMetadata
import no.ks.kes.lib.SerializationId
import no.ks.kes.serdes.proto.ProtoEvent
import no.ks.svarut.event.Avsender
import java.util.*

private val log = KotlinLogging.logger {}

data class KontoAggregate(
        val aggregateId: UUID,
        val aktivert: Boolean = false
) : Aggregate

object Konto: AggregateConfiguration<KontoAggregate>("konto") {

    init {
        init { avsenderOpprettet: AvsenderOpprettet, aggregatId: UUID ->
            log.info { "Avsender opprettet $aggregatId" }
            KontoAggregate(aggregatId)
        }

        apply<AvsenderAktivert> {
            copy(
                    aktivert = true
            )
        }

        apply<AvsenderDeaktivert> {
            copy(
                    aktivert = false
            )
        }
    }

    data class DemoEventMetadata(override val aggregateId: UUID, val occurredOn: Long): EventMetadata(aggregateId)

    @SerializationId("Avsender.AvsenderOpprettet")
    data class AvsenderOpprettet(override val msg: Avsender.AvsenderOpprettet) :
        ProtoEvent<KontoAggregate, Avsender.AvsenderOpprettet>

    @SerializationId("Avsender.AvsenderAktivert")
    data class AvsenderAktivert(override val msg: Avsender.AvsenderAktivert) :
        ProtoEvent<KontoAggregate, Avsender.AvsenderAktivert>

    @SerializationId("Avsender.AvsenderDeaktivert")
    data class AvsenderDeaktivert(override val msg: Avsender.AvsenderDeaktivert) :
        ProtoEvent<KontoAggregate,Avsender.AvsenderDeaktivert>
}


