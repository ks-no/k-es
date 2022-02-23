package no.ks.kes.grpc


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
    Unknown
}