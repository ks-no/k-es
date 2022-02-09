package no.ks.kes.grpc

import io.grpc.Status


/**
 * Exception thrown when subscription is dropped
 */
class GrpcSubscriptionDroppedException(val reason: GrpcSubscriptionDroppedReason, cause: Exception?) :
    RuntimeException("Subscription was dropped. Reason: $reason", cause) {
        constructor(reason: GrpcSubscriptionDroppedReason) : this(reason, null)
    }

enum class GrpcSubscriptionDroppedReason {
    SubscriptionCancelled,
    ConnectionShutDown,
    GrpcStatusException,
    Unknown
}