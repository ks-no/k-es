package no.ks.kes.esjc

import com.github.msemys.esjc.ResolvedEvent
import java.util.*

object EsjcEventUtil {
    internal fun isIgnorableEvent(resolvedEvent: ResolvedEvent): Boolean =
            with(resolvedEvent) {
                event == null ||
                        event.eventType == null ||
                        event.eventType.isEmpty() ||
                        event.eventType.startsWith("$")
            }

    fun defaultStreamName(domain: String): (aggregateType: String, aggregateId: UUID) -> String = { t, id ->
        if (domain.isBlank() || t.isBlank())
            throw IllegalArgumentException("Invalid stream name. No stream name components can be null or empty")
        else
            "$domain-$t-$id"
    }
}