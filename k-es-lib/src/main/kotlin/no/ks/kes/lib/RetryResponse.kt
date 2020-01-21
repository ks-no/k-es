package no.ks.kes.lib

import java.time.Instant

class RetryResponse<A : Aggregate> {
    constructor(failedEvent: Event<A>) {
        this.failedEvent = failedEvent
        nextExecution = null
    }

    constructor(nextExecution: Instant) {
        failedEvent = null
        this.nextExecution = nextExecution
    }

    val failedEvent: Event<*>?

    val nextExecution: Instant?
}