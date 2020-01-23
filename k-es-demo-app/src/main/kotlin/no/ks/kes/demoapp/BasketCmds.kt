package no.ks.kes.demoapp

import no.ks.kes.lib.AggregateRepository
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdHandler
import no.ks.kes.lib.CmdHandler.Result.*
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.util.*

class BasketCmds(repo: AggregateRepository, paymentProcessor: PaymentProcessor) : CmdHandler<Basket>(repo) {
    override fun initAggregate(): Basket = Basket()

    @SerializationId("BasketCreate")
    data class Create(override val aggregateId: UUID) : Cmd<Basket>

    @SerializationId("BasketAddItem")
    data class AddItem(override val aggregateId: UUID, val itemId: UUID) : Cmd<Basket>

    @SerializationId("BasketCheckOut")
    data class CheckOut(override val aggregateId: UUID) : Cmd<Basket>

    init {
        initOn<Create> { Succeed(Basket.Created(it.aggregateId, Instant.now())) }

        on<AddItem> {
            if (basketClosed)
                Fail(IllegalStateException("Can't add items to a closed basket"))
            else
                Succeed(Basket.ItemAdded(it.aggregateId, Instant.now(), it.itemId))
        }

        on<CheckOut> {
            if (basketClosed) {
                Fail(IllegalStateException("Can't check out a closed basket"))
            } else if (basket.isEmpty())
                Fail(IllegalStateException("Can't check out a empty basket, buy something first?"))
            else {
                try {
                    paymentProcessor.process(it.aggregateId)
                    Succeed(Basket.CheckedOut(it.aggregateId, Instant.now(), basket.toMap()))
                } catch (e: Exception) {
                    RetryOrFail<Basket>(e)
                }
            }
        }
    }
}

interface PaymentProcessor {
    fun process(orderId: UUID)
}
