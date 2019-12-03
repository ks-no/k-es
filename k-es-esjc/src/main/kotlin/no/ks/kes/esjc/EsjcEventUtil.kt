package no.ks.kes.esjc

import com.github.msemys.esjc.ResolvedEvent

internal object EsjcEventUtil {
    fun isIgnorableEvent(resolvedEvent: ResolvedEvent): Boolean =
            with(resolvedEvent) {
                event == null ||
                event.eventType == null ||
                event.eventType.isEmpty() ||
                event.eventType.startsWith("$")
            }
}