package no.ks.kes.demoapp

import no.ks.kes.lib.AggregateRepository
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdHandler
import no.ks.kes.lib.CmdHandler.Result.*
import no.ks.kes.lib.SerializationId
import java.util.*

class KontoCmds(repo: AggregateRepository) : CmdHandler<KontoAggregate>(repo, Konto) {

    init {
        init<Opprett> { Succeed(Konto.AvsenderOpprettet(Konto.DemoEventMetadata(it.aggregateId, System.currentTimeMillis()), it.orgId)) }

        apply<Aktiver> {
            if (aktivert)
                Fail(IllegalStateException("Konto er allerede aktivert"))
            else {
                Succeed(Konto.AvsenderAktivert(Konto.DemoEventMetadata(it.aggregateId, System.currentTimeMillis())))
            }
        }

        apply<Deaktiver> {
            if (!aktivert)
                Fail(IllegalStateException("Konto er allerede deaktivert"))
            else {
                Succeed(Konto.AvsenderDeaktivert(Konto.DemoEventMetadata(it.aggregateId, System.currentTimeMillis())))
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