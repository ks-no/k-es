package no.ks.kes.grpc


/**
 * Exception thrown when subscription is dropped
 */
class GrpcSubscriptionException(val reason: GrpcSubscriptionReason, cause: Exception?) :
    RuntimeException("Subscription was dropped. Reason: $reason", cause) {
    constructor(reason: GrpcSubscriptionReason) : this(reason, null)
}

enum class GrpcSubscriptionReason {
    ConnectionShutDown,
    Unknown
}