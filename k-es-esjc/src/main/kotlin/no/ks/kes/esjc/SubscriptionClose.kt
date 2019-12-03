package no.ks.kes.esjc

import com.github.msemys.esjc.SubscriptionDropReason

data class SubscriptionClose(val subscriptionDropReason: SubscriptionDropReason, val exception: Exception)