package no.ks.kes.demoapp

import no.ks.kes.lib.*
import no.ks.kes.lib.CmdHandler.Result.*
import java.util.*

class BasketCmds(repo: AggregateRepository, paymentProcessor: PaymentProcessor) : CmdHandler<BasketAggregate>(repo, Basket) {

    init {
        init<Create> { Succeed(
            Event( eventData = Basket.Created(it.aggregateId), aggregateId = it.aggregateId)) }

        apply<AddItem> {
            if (basketClosed)
                Fail(IllegalStateException("Can't add items to a closed basket"))
            else
                Succeed(Event(eventData = Basket.ItemAdded(it.aggregateId, it.itemId), aggregateId = it.aggregateId))
        }

        apply<CheckOut> {
            when {
                basketClosed -> Fail(IllegalStateException("Can't check out a closed basket"))
                basketContents.isEmpty() -> Fail(IllegalStateException("Can't check out a empty basket, buy something first?"))
                else -> try {
                    paymentProcessor.process(it.aggregateId)
                    Succeed(
                        Event( eventData = Basket.CheckedOut(it.aggregateId, basketContents.toMap()), aggregateId = it.aggregateId))
                } catch (e: Exception) {
                    RetryOrFail<BasketAggregate>(e)
                }
            }
        }
    }

    @SerializationId("BasketCreate")
    data class Create(override val aggregateId: UUID) : Cmd<BasketAggregate>

    @SerializationId("BasketAddItem")
    data class AddItem(override val aggregateId: UUID, val itemId: UUID) : Cmd<BasketAggregate>

    @SerializationId("BasketCheckOut")
    data class CheckOut(override val aggregateId: UUID) : Cmd<BasketAggregate>
}

interface PaymentProcessor {
    fun process(orderId: UUID)
}
