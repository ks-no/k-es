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


## Projections

## Sagas and the Command Queue