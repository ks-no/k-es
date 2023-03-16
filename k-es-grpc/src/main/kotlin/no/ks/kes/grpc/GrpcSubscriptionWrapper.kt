package no.ks.kes.grpc

import no.ks.kes.lib.EventSubscription

/**
 * Wraps Grpc Subscription
 */
class GrpcSubscriptionWrapper(val streamId: String, private val lastEventNumberSupplier: () -> Long): EventSubscription {

    val isSubscribedToAll: Boolean
        get() = streamId == "\$all";

    override fun lastProcessedEvent(): Long = lastEventNumberSupplier.invoke()
}