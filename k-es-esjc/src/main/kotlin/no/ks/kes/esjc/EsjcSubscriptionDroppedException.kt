package no.ks.kes.esjc

import com.github.msemys.esjc.SubscriptionDropReason

/**
 * Exception thrown when subscription is dropped
 */
class EsjcSubscriptionDroppedException(val reason: SubscriptionDropReason, cause: Exception) : RuntimeException("Subscription was dropped. Reason: $reason", cause)