package no.ks.kes.grpc


class GrpcSubscriptionException(reason: GrpcSubscriptionExceptionReason, cause: Exception?) :
    RuntimeException("Subscription failed. Reason: $reason", cause) {
}

enum class GrpcSubscriptionExceptionReason {
    ConnectionShutDown,
    Unknown
}