package no.ks.kes.demoapp

import no.ks.kes.esjc.EsjcAggregateRepository
import no.ks.kes.lib.AggregateRepository
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.CmdHandler
import no.ks.kes.lib.SerializationId
import java.time.Instant
import java.util.*

class BasketCmds(repo: AggregateRepository, paymentProcessor: PaymentProcessor) : CmdHandler<Basket>(repo) {
    override fun initAggregate(): Basket = Basket()

    @SerializationId("StartSession")
    data class StartSession(override val aggregateId: UUID) : Cmd<Basket>

    @SerializationId("AddItemToBasket")
    data class AddItemToBasket(override val aggregateId: UUID, val itemId: UUID) : Cmd<Basket>

    @SerializationId("CheckOutBasket")
    data class CheckOutBasket(override val aggregateId: UUID) : Cmd<Basket>

    init {
        initOn<StartSession> { Result.Succeed(SessionStarted(it.aggregateId, Instant.now())) }

        on<AddItemToBasket> {
            if (basketClosed)
                Result.Fail(IllegalStateException("Can't add items to a closed basket"))
            else
                Result.Succeed(ItemAddedToBasket(it.aggregateId, Instant.now(), it.itemId))
        }
        on<CheckOutBasket> {
            if (basketClosed) {
                Result.Fail(IllegalStateException("Can't check out a closed basket"))
            } else {
                try {
                    paymentProcessor.process(it.aggregateId)
                    Result.Succeed(BasketCheckedOut(it.aggregateId, Instant.now(), basket.toMap()))
                } catch (e: Exception) {
                    Result.RetryOrFail<Basket>(e)
                }
            }
        }
    }
}

interface PaymentProcessor {
    fun process(orderId: UUID)
}
