package no.ks.kes.grpc


class GrpcSubscriptionException(reason: GrpcSubscriptionReason, cause: Exception?) :
    RuntimeException("Subscription failed. Reason: $reason", cause) {
}

enum class GrpcSubscriptionReason {
    ConnectionShutDown,
    Unknown
}