package no.ks.kes.grpc

import com.eventstore.dbclient.ResolvedEvent
import java.util.*

object GrpcEventUtil {

    fun defaultStreamName(domain: String): (aggregateType: String, aggregateId: UUID) -> String = { t, id ->
        if (domain.isBlank() || t.isBlank())
            throw IllegalArgumentException("Invalid stream name. No stream name components can be null or empty")
        else
            "$domain-$t-$id"
    }

    fun ResolvedEvent.isResolved(): Boolean = link != null && event != null

    fun ResolvedEvent.isIgnorable(): Boolean =
        event == null ||
        event.eventType == null ||
        event.eventType.isEmpty() ||
        event.eventType.startsWith("$")

}