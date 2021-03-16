package no.ks.kes.demoapp

import no.ks.kes.lib.AggregateRepository
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdHandler
import no.ks.kes.lib.CmdHandler.Result.*
import no.ks.kes.lib.SerializationId
import java.util.*

class KontoCmds(repo: AggregateRepository) : CmdHandler<KontoAggregate>(repo, Konto) {

    init {
        init<Opprett> { Succeed(Konto.AvsenderOpprettet(it.aggregateId, it.orgId)) }

        apply<Aktiver> {
            if (aktivert)
                Fail(IllegalStateException("Konto er allerede aktivert"))
            else {
                Succeed(Konto.AvsenderAktivert(it.aggregateId))
            }
        }

        apply<Deaktiver> {
            if (!aktivert)
                Fail(IllegalStateException("Konto er allerede deaktivert"))
            else {
                Succeed(Konto.AvsenderDeaktivert(it.aggregateId))
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