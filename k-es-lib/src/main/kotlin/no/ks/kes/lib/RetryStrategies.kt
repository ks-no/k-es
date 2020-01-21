package no.ks.kes.lib

import java.time.Instant
import java.time.temporal.ChronoUnit

object RetryStrategies {
    val DEFAULT = { i: Int ->
        when (i) {
            0 -> Instant.now().plus(10, ChronoUnit.SECONDS)
            1 -> Instant.now().plus(1, ChronoUnit.MINUTES)
            2 -> Instant.now().plus(10, ChronoUnit.MINUTES)
            else -> null

        }
    }

}
