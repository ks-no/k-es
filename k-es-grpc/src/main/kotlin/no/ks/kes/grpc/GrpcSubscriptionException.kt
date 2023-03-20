package no.ks.kes.grpc


class GrpcSubscriptionException(cause: GrpcSubscriptionCause, exception: Exception?) :
    RuntimeException("Subscription failed. Cause: $cause", exception) {
}

enum class GrpcSubscriptionCause {
    ConnectionShutDown,
    Unknown
}