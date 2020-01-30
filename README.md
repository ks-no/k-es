# k-es

Kotlin library for persistance through event-sourced aggregates. Support for projections and sagas included, check out the demo-app to see how it all fits together.

Currently supports ESJC (netty based client for [eventstore.org](https://eventstore.org/)) as a aggregate repository and Microsoft Sql Server as a command and saga repository. 

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
Aggregate classes extend the abstract `Aggregate` superclass. An aggregateType must be specified, and the aggregate state typically consists of variables which are mutated as the aggregates events play out.   

```kotlin
class Basket : Aggregate() {

    override val aggregateType = "basket"
    var aggregateId: UUID? = null
    var basket: MutableMap<UUID, Int> = mutableMapOf()
    var basketClosed: Boolean = false

    init {
        on<Created> {
            aggregateId = it.aggregateId
        }

        on<ItemAdded> {
            basket.compute(it.itemId) { _, count -> count?.inc() ?: 1 }
        }

        on<CheckedOut> {
            basketClosed = true
        }
    }
}
```

## Commands and Command Handlers
Aggregates are mutated by submitting commands to an associated command handler. The command handler will retrieve the aggregate from the event-store, play out any pre-existing events, and apply the logic defined in the `initOn` or the `on` functions to the current aggregate state: any derived events are appended to the aggregates event log in the store. The command handler functions will often involve side effects, such as invoking external api's. Clients for these can be injected into the command handler. 

```kotlin
class BasketCmds(repo: AggregateRepository) : CmdHandler<Basket>(repo) {
    override fun initAggregate(): Basket = Basket()

    @SerializationId("BasketCreate")
    data class Create(override val aggregateId: UUID) : Cmd<Basket>

    @SerializationId("BasketAddItem")
    data class AddItem(override val aggregateId: UUID, val itemId: UUID) : Cmd<Basket>

    init {
        initOn<Create> { Succeed(Basket.Created(it.aggregateId, Instant.now())) }

        on<AddItem> {
            if (basketClosed)
                Fail(IllegalStateException("Can't add items to a closed basket"))
            else
                Succeed(Basket.ItemAdded(it.aggregateId, Instant.now(), it.itemId))
        }
    }
}
```

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

## Sagas and the Command Queue

## Bringing it all together

![Overview](https://www.lucidchart.com/publicSegments/view/895679bf-a290-46cd-b77e-c32b7f37ce52/image.png)

