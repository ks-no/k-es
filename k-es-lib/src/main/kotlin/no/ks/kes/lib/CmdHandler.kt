package no.ks.kes.lib

import mu.KotlinLogging
import java.time.Instant
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

@Suppress("UNCHECKED_CAST")
abstract class CmdHandler<A : Aggregate>(private val repository: AggregateRepository, aggregateConfiguration: AggregateConfiguration<A>) {

    protected val applicators = mutableMapOf<KClass<Cmd<A>>, (a: A, c: Cmd<A>) -> Result<A>>()
    protected val initializers = mutableMapOf<KClass<Cmd<A>>, (c: Cmd<A>) -> Result<A>>()
    private val validatedAggregateConfiguration = aggregateConfiguration.getConfiguration { repository.getSerializationId(it) }


    protected inline fun <reified C : Cmd<A>> apply(crossinline handler: A.(C) -> Result<A>) {
        check(!applicators.containsKey(C::class as KClass<Cmd<A>>)) { "There are multiple \"apply\" configurations for the command ${C::class.simpleName} in the command handler ${this::class.simpleName}, only a single \"apply\" handler is allowed for each command" }
        applicators[C::class as KClass<Cmd<A>>] = { a, c -> handler.invoke(a, c as C) }
    }

    protected inline fun <reified C : Cmd<A>> init(crossinline handler: (C) -> Result<A>) {
        check(!initializers.containsKey(C::class as KClass<Cmd<A>>)) { "There are multiple \"init\" configurations for the command ${C::class.simpleName} in the command handler ${this::class.simpleName}, only a single \"init\" handler is allowed for each command" }
        initializers[C::class as KClass<Cmd<A>>] = { c -> handler.invoke(c as C) }
    }

    fun handledCmds(): Set<KClass<Cmd<A>>> = applicators.keys.toSet() + initializers.keys.toSet()

    @Synchronized
    fun handle(cmd: Cmd<A>): A {
        val readResult = readAggregate(cmd)

        return when (val result = invokeHandler(cmd, readResult)) {
            is Result.Fail,
            is Result.RetryOrFail,
            is Result.Error ->
                throw result.exception!!
            is Result.Succeed<A> -> {
                appendDerivedEvents(validatedAggregateConfiguration.aggregateType, readResult, cmd, result.derivedEvents)
                result.derivedEvents.fold(
                        when (readResult) {
                            is AggregateReadResult.InitializedAggregate<*> -> readResult.aggregateState as A
                            is AggregateReadResult.NonExistingAggregate, is AggregateReadResult.UninitializedAggregate -> null
                        },
                        { a, e ->
                            validatedAggregateConfiguration.applyEvent(EventWrapper(
                                    event = e,
                                    eventNumber = -1,
                                    serializationId = repository.getSerializationId(e::class as KClass<Event<*>>)
                            ), a)
                        })
                        ?: error("applying derived events to the aggregate resulted in a null-state!")
            }
        }
    }

    @Synchronized
    fun handleAsync(cmd: Cmd<*>, retryNumber: Int): AsyncResult {
        val readResult = readAggregate(cmd as Cmd<A>)

        return when (val result = invokeHandler(cmd, readResult)) {
            is Result.Fail<A> -> {
                appendDerivedEvents(validatedAggregateConfiguration.aggregateType, readResult, cmd, result.events)
                log.error("execution of ${cmd::class.simpleName} failed permanently: $cmd", result.exception!!); AsyncResult.Fail
            }
            is Result.RetryOrFail<A> -> {
                val nextExecution = result.retryStrategy.invoke(retryNumber)
                log.error("execution of ${cmd::class.simpleName} failed with retry, ${nextExecution?.let { "next retry at $it" }
                        ?: " but all retries are exhausted"}", result.exception!!)
                if (nextExecution == null) {
                    appendDerivedEvents(validatedAggregateConfiguration.aggregateType, readResult, cmd, result.events)
                    AsyncResult.Fail
                } else {
                    AsyncResult.Retry(nextExecution)
                }
            }
            is Result.Succeed<A> -> {
                appendDerivedEvents(validatedAggregateConfiguration.aggregateType, readResult, cmd, result.derivedEvents)
                AsyncResult.Success
            }
            is Result.Error<A> -> {
                AsyncResult.Error(result.exception!!)
            }
        }
    }

    private fun appendDerivedEvents(aggregateType: String, readResult: AggregateReadResult, cmd: Cmd<A>, events: List<Event<A>>) {
        if (events.isNotEmpty())
            repository.append(aggregateType, cmd.aggregateId, resolveExpectedEventNumber(readResult, cmd.useOptimisticLocking()), events)
    }

    private fun resolveExpectedEventNumber(readResult: AggregateReadResult, useOptimisticLocking: Boolean): ExpectedEventNumber =
            when (readResult) {
                is AggregateReadResult.NonExistingAggregate -> ExpectedEventNumber.AggregateDoesNotExist
                is AggregateReadResult.InitializedAggregate<*> -> if (useOptimisticLocking) ExpectedEventNumber.Exact(readResult.eventNumber) else ExpectedEventNumber.AggregateExists
                is AggregateReadResult.UninitializedAggregate -> if (useOptimisticLocking) ExpectedEventNumber.Exact(readResult.eventNumber) else ExpectedEventNumber.AggregateExists
            }

    private fun readAggregate(cmd: Cmd<A>): AggregateReadResult =
            repository.read(cmd.aggregateId, validatedAggregateConfiguration)

    private fun invokeHandler(cmd: Cmd<A>, readResult: AggregateReadResult): Result<A> =
            when (readResult) {
                is AggregateReadResult.NonExistingAggregate, is AggregateReadResult.UninitializedAggregate -> {
                    initializers[cmd::class]
                            ?.run {
                                try {
                                    invoke(cmd)
                                } catch (e: Exception) {
                                    Result.Error<A>(e)
                                }
                            }
                            ?: error("Aggregate ${cmd.aggregateId} does not exist, and cmd ${cmd::class.simpleName} is not configured as an initializer. Consider adding an \"init\" configuration for this command.")
                }
                is AggregateReadResult.InitializedAggregate<*> -> {
                    applicators[cmd::class ]
                            ?.run {
                                try {
                                    invoke(readResult.aggregateState as A, cmd)
                                } catch (e: Exception) {
                                    Result.Error<A>(e)
                                }
                            }
                            ?: error("No handler found for cmd ${cmd::class.simpleName}")
                }
            }


    sealed class Result<A : Aggregate>(val exception: Exception?) {

        class Fail<A : Aggregate> private constructor(exception: Exception, val events: List<Event<A>>) : Result<A>(exception) {
            constructor(event: Event<A>, exception: Exception) : this(exception, listOf(event))
            constructor(events: List<Event<A>>, exception: Exception) : this(exception, events)
            constructor(exception: Exception) : this(exception, emptyList())
        }

        class RetryOrFail<A : Aggregate> private constructor(exception: Exception, val events: List<Event<A>>, val retryStrategy: (Int) -> Instant?) : Result<A>(exception) {
            constructor(event: Event<A>, exception: Exception, retryStrategy: (Int) -> Instant?) : this(exception, listOf(event), retryStrategy)
            constructor(events: List<Event<A>>, exception: Exception, retryStrategy: (Int) -> Instant?) : this(exception, events, retryStrategy)
            constructor(exception: Exception, retryStrategy: (Int) -> Instant?) : this(exception, emptyList(), retryStrategy)
            constructor(event: Event<A>, exception: Exception) : this(exception, listOf(event), RetryStrategies.DEFAULT)
            constructor(events: List<Event<A>>, exception: Exception) : this(exception, events, RetryStrategies.DEFAULT)
            constructor(exception: Exception) : this(exception, emptyList(), RetryStrategies.DEFAULT)
        }

        internal class Error<A : Aggregate>(exception: Exception) : Result<A>(exception)

        class Succeed<A : Aggregate>(val derivedEvents: List<Event<A>>) : Result<A>(null) {
            constructor(event: Event<A>) : this(listOf(event))
            constructor() : this(emptyList())
        }
    }

    sealed class AsyncResult {
        object Success : AsyncResult()
        object Fail : AsyncResult()
        class Retry(val nextExecution: Instant) : AsyncResult()
        class Error(val exception: Exception) : AsyncResult()
    }
}