package no.ks.kes.esjc

import com.github.msemys.esjc.CatchUpSubscriptionListener
import com.github.msemys.esjc.CatchUpSubscriptionSettings
import com.github.msemys.esjc.EventStore
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

class EsjcEventSubscriber(private val eventStore: EventStore) {
    fun subscribeByCategory(category: String, hwm: Long, listener: CatchUpSubscriptionListener) =
            eventStore.subscribeToStreamFrom(
                    "\$ce-$category",
                    hwm,
                    CatchUpSubscriptionSettings.newBuilder().resolveLinkTos(true).build(),
                    listener
            ).also {
                log.info("Subscription initiated from event number $hwm on category projection $category")
            }!!
}