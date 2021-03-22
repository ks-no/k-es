package no.ks.kes.demoapp

import no.ks.kes.lib.*
import no.ks.kes.lib.CmdHandler.Result.*
import no.ks.svarut.event.Avsender
import java.util.*

class KontoCmds(repo: AggregateRepository) : CmdHandler<KontoAggregate>(repo, Konto) {

    init {
        init<Opprett> { Succeed(
            EventData(
                aggregateId = it.aggregateId,
                event = Konto.AvsenderOpprettet(Avsender.AvsenderOpprettet.newBuilder().setOrgId(it.orgId).build()),
                metadata = Konto.DemoMetadata(it.aggregateId, System.currentTimeMillis())
            ))
        }

        apply<Aktiver> {
            if (aktivert)
                Fail(IllegalStateException("Konto er allerede aktivert"))
            else {
                Succeed(
                    EventData(
                        aggregateId = it.aggregateId,
                        event = Konto.AvsenderAktivert(Avsender.AvsenderAktivert.newBuilder().build()),
                        metadata = Konto.DemoMetadata(it.aggregateId, System.currentTimeMillis())
                    ))
            }
        }

        apply<Deaktiver> {
            if (!aktivert)
                Fail(IllegalStateException("Konto er allerede deaktivert"))
            else {
                Succeed(
                    EventData(
                        aggregateId = it.aggregateId,
                        event = Konto.AvsenderDeaktivert(Avsender.AvsenderDeaktivert.newBuilder().build()),
                        metadata = Konto.DemoMetadata(it.aggregateId, System.currentTimeMillis())
                    ))
            }
        }
    }

    @SerializationId("OpprettKonto")
    data class Opprett(override val aggregateId: UUID, val orgId: String) : Cmd<KontoAggregate>

    @SerializationId("AktiverKonto")
    data class Aktiver(override val aggregateId: UUID) : Cmd<KontoAggregate>

    @SerializationId("DeaktiverKonto")
    data class Deaktiver(override val aggregateId: UUID) : Cmd<KontoAggregate>
}