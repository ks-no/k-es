# k-es

Kotlin library for persistance through [event-sourced](https://martinfowler.com/eaaDev/EventSourcing.html) aggregates. Support for projections and sagas included, check out the demo-app to see how it all fits together.

Currently supports [ESJC](https://github.com/msemys/esjc) (a netty based client for [eventstore.org](https://eventstore.org/)) as the event store, Microsoft Sql Server as a command and saga repository, and JSON/Jackson as a serialization method, but a modular design should allow other databases and serialization types to be supported in the future. 

## Events
Events classes must implement the `Event<Aggregate>` interface and be annotated with `@SerializationId".  
```kotlin
    @SerializationId("BasketCreated")
    data class Created(override val aggregateId: UUID, override val timestamp: Instant) : Event<Basket>
```

* _SerializationId_: This id is used for mapping serialized events to java classes. Must be unique for all events in the system.
* _aggregateId_: The specific instance of the aggregate associated with the event.
* _timestamp_: The of the event occurrence.

As time moves on the system (and the world it describes) changes, and altering or replacing events might become necessary. K-ES supports this through event upgrading: an event may implement an upgrade function which will be invoked when the event is read from the event-store, either through projection or saga subscriptions or when restoring aggregates. It is also recommended to tag the older version with the `@Deprecated` annotation, which will make it easier to identify usages of the replaced event. K-ES will throw an error if an aggregate, projection or saga subscribe to a deprecated event.  

```kotlin
    @SerializationId("BasketSessionStarted")
    @Deprecated("This event has been replaced by a newer version", replaceWith = ReplaceWith("Basket.Created(aggregateId, timestamp)"), level = DeprecationLevel.ERROR)
    data class SessionStarted(override val aggregateId: UUID, override val timestamp: Instant) : Event<Basket> {
        override fun upgrade(): Event<Basket>? {
            return Created(aggregateId, timestamp)
        }
    }
```
## Aggregates
Aggregates are created by extending the `AggregateConfiguration` class, and using `init` and `apply` handlers to incrementally build a state from the events in the aggregates stream.   

```kotlin
data class BasketAggregate(
        val aggregateId: UUID,
        val basketContents: Map<UUID, Int> = emptyMap(),
        val basketClosed: Boolean = false
) : Aggregate

object Basket : AggregateConfiguration<BasketAggregate>("basket") {

    init {
        init<Created> {
            BasketAggregate(
                    aggregateId = it.aggregateId
            )
        }

        apply<ItemAdded> {
            copy(
                    basketContents = basketContents + (it.itemId to basketContents.getOrDefault(it.itemId, 0).inc())
            )
        }

        apply<CheckedOut> {
            copy(
                    basketClosed = true
            )
        }
    }
}
```

## Commands and Command Handlers
Aggregates are mutated by submitting commands to an associated command handler. The command handler will retrieve the aggregate from the event-store, play out any pre-existing events, and apply the logic defined in the `init` or `apply` functions to the current aggregate state: any derived events are appended to the aggregates event log in the store. 

The command handler functions will often involve side effects, such as invoking external api's. Clients for these can be injected into the command handler. 

```kotlin
class BasketCmds(repo: AggregateRepository, paymentProcessor: PaymentProcessor) : CmdHandler<BasketAggregate>(repo, Basket) {

    init {
        init<Create> { Succeed(Basket.Created(it.aggregateId, Instant.now())) }

        apply<AddItem> {
            if (basketClosed)
                Fail(IllegalStateException("Can't add items to a closed basket"))
            else
                Succeed(Basket.ItemAdded(it.aggregateId, Instant.now(), it.itemId))
        }

        apply<CheckOut> {
            when {
                basketClosed -> Fail(IllegalStateException("Can't check out a closed basket"))
                basketContents.isEmpty() -> Fail(IllegalStateException("Can't check out a empty basket, buy something first?"))
                else -> try {
                    paymentProcessor.process(it.aggregateId)
                    Succeed(Basket.CheckedOut(it.aggregateId, Instant.now(), basketContents.toMap()))
                } catch (e: Exception) {
                    RetryOrFail<BasketAggregate>(e)
                }
            }
        }
    }
}
```

Commands can be passed to a command handler through the `handle` or `handleAsync` method. The first is adapted to synchronous invocations where the command result is required right away, such as a user action, the latter to asynchronous invocations. Commands retrieved from the command queue use `handleAsync` 

Each command can complete in four different ways:
* By returning `Fail`. This signals that the command should fail permanently with an included exception and an optional event. If the command has been invoked synchronously the exception will be thrown, if the invocation was asynchronous the event will be appended to the relevant aggregate.  
* By returning `RetryOrFail` This signals that the command should fail permanently, unless the specified retry strategy determines that another attempt should be performed.
* By returning `Success` This signals that the command is successful. If an event is specified it will be appended to the aggregate.
* By throwing a exception. If the command execution throws an uncaught exception the command will be market as `Error` in the command queue, and no more attempts to execute will be performed. This will effectivly block further command execution on this aggregate until this status is manually lifted.
 
## Projections
Projections subscribe to events from the event store, and build state based on these. 
```kotlin
class Shipments : Projection() {
    private val failed: MutableSet<UUID> = mutableSetOf()

    init {
        on<Shipment.Failed> { failed.add(it.basketId) }
    }

    fun isFailedShipment(basketId: UUID): Boolean = failed.contains(basketId)
}
```

## Sagas

## Bringing it all together

![Overview](https://www.lucidchart.com/publicSegments/view/895679bf-a290-46cd-b77e-c32b7f37ce52/image.png)

