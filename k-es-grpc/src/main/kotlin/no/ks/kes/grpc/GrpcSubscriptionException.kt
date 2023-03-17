package no.ks.kes.grpc


class GrpcSubscriptionException(val reason: GrpcSubscriptionReason, cause: Exception?) :
    RuntimeException("Subscription was dropped. Reason: $reason", cause) {
    constructor(reason: GrpcSubscriptionReason) : this(reason, null)
}

enum class GrpcSubscriptionReason {
    ConnectionShutDown,
    Unknown
}