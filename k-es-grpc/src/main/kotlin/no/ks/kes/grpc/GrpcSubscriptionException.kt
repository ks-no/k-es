package no.ks.kes.grpc


class GrpcSubscriptionException(cause: GrpcSubscriptionCause, exception: Exception?) :
    RuntimeException("Subscription failed. Reason: $cause", exception) {
}

enum class GrpcSubscriptionCause {
    ConnectionShutDown,
    Unknown
}