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
        init<AvsenderOpprettet> {
            log.info { "Avsender opprettet $it" }
            KontoAggregate(
                    aggregateId = it.aggregateId
            )
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

    data class SvarUtMetadata(override val aggregateId: UUID, val occurredOn: Long): EventMetadata(aggregateId) {
    }

    @SerializationId("Avsender.AvsenderOpprettet")
    data class AvsenderOpprettet(override val aggregateId: UUID,val orgId: String) :
        ProtoEvent<KontoAggregate> {

        override fun getMsg() = Avsender.AvsenderOpprettet.newBuilder()
            .setOrgId(orgId)
            .build()

        override fun metadata(): EventMetadata = SvarUtMetadata(aggregateId = aggregateId, System.currentTimeMillis())
    }

    @SerializationId("Avsender.AvsenderAktivert")
    data class AvsenderAktivert(override val aggregateId: UUID) :
        ProtoEvent<KontoAggregate> {

        override fun getMsg() = Avsender.AvsenderAktivert.newBuilder().build()
        override fun metadata(): EventMetadata = EventMetadata(aggregateId = aggregateId)
    }

    @SerializationId("Avsender.AvsenderDeaktivert")
    data class AvsenderDeaktivert(override val aggregateId: UUID) :
        ProtoEvent<KontoAggregate> {

        override fun getMsg() = Avsender.AvsenderDeaktivert.newBuilder().build()
        override fun metadata(): EventMetadata = EventMetadata(aggregateId = aggregateId)

    }
}


