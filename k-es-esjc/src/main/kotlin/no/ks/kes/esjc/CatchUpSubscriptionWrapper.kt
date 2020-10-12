package no.ks.kes.esjc

import com.github.msemys.esjc.CatchUpSubscription
import no.ks.kes.lib.EventSubscription

/**
 * Wraps esjc CatchUpSubscription
 */
class CatchUpSubscriptionWrapper(private val catchUpSubscription: CatchUpSubscription): EventSubscription {

    val streamId: String?
        get() = catchUpSubscription.streamId

    val isSubscribedToAll: Boolean
        get() = catchUpSubscription.isSubscribedToAll

    override fun lastProcessedEvent(): Long = catchUpSubscription.lastProcessedEventNumber()

}