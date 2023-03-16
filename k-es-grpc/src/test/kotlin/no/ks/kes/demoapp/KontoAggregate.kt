package no.ks.kes.demoapp

import mu.KotlinLogging
import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.AggregateConfiguration
import no.ks.kes.lib.Metadata
import no.ks.kes.lib.SerializationId
import no.ks.kes.serdes.proto.ProtoEventData
import no.ks.svarut.event.Avsender
import java.util.*

private val log = KotlinLogging.logger {}

data class KontoAggregate(
        val aggregateId: UUID,
        val aktivert: Boolean = false
) : Aggregate

object Konto: AggregateConfiguration<KontoAggregate>("konto") {

    init {
        init { _: AvsenderOpprettet, aggregateId: UUID ->
            log.info { "Avsender opprettet $aggregateId" }
            KontoAggregate(aggregateId)
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

    data class DemoMetadata(val aggregateId: UUID, val occurredOn: Long): Metadata

    @SerializationId("Avsender.AvsenderOpprettet")
    data class AvsenderOpprettet(override val msg: Avsender.AvsenderOpprettet) :
        ProtoEventData<KontoAggregate>

    @SerializationId("Avsender.AvsenderAktivert")
    data class AvsenderAktivert(override val msg: Avsender.AvsenderAktivert) :
        ProtoEventData<KontoAggregate>

    @SerializationId("Avsender.AvsenderDeaktivert")
    data class AvsenderDeaktivert(override val msg: Avsender.AvsenderDeaktivert) :
        ProtoEventData<KontoAggregate>
}