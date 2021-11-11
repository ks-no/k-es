package no.ks.kes.grpc

import com.github.msemys.esjc.SubscriptionDropReason

/**
 * Exception thrown when subscription is dropped
 */
class GrpcSubscriptionDroppedException(val reason: SubscriptionDropReason, cause: Exception) : RuntimeException("Subscription was dropped. Reason: $reason", cause)