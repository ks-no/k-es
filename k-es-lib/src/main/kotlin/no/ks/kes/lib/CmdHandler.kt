package no.ks.kes.lib

import mu.KotlinLogging
import java.time.Instant
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}

abstract class CmdHandler<A : Aggregate>(private val repository: AggregateRepository) {

    protected val handlers = mutableSetOf<OnCmd<A>>()
    protected val initializers = mutableSetOf<InitOnCmd<A>>()

    protected inline fun <reified C : Cmd<A>> on(crossinline handler: A.(C) -> Result<A>) {
        handlers.add(OnCmd(C::class as KClass<Cmd<A>>) { a, c -> handler.invoke(a, c as C) })
    }

    protected inline fun <reified C : Cmd<A>> initOn(crossinline handler: (C) -> Result<A>) {
        initializers.add(InitOnCmd(C::class as KClass<Cmd<A>>) { c -> handler.invoke(c as C) })
    }

    fun handledCmds(): Set<KClass<Cmd<A>>> = (handlers.map { it.cmdClass } + initializers.map { it.cmdClass }).toSet()

    @Synchronized
    fun handle(cmd: Cmd<A>): A {
        val aggregate = readAggregate(cmd)

        when (val result = invokeHandler(cmd, aggregate)) {
            is Result.Fail<A>, is Result.RetryOrFail<A> ->
                throw result.exception!!
            is Result.Succeed<A> -> {
                write(aggregate, cmd, result.events)
                return result.events.fold(aggregate) { a: A, e: Event<A> -> a.applyEvent(e, Long.MIN_VALUE) }
            }
        }
    }

    @Synchronized
    fun handleAsync(cmd: Cmd<*>, retryNumber: Int): AsyncResult {
        val aggregate = readAggregate(cmd as Cmd<A>)

        return when (val result = invokeHandler(cmd, aggregate)) {
            is Result.Fail<A> -> {
                log.error("asdf", result.exception!!); AsyncResult.Fail
            }
            is Result.RetryOrFail<A> -> {
                val nextExecution = result.retryStrategy.invoke(retryNumber)
                log.error("asdf", result.exception!!)
                if (nextExecution == null) {
                    write(aggregate, cmd, result.events)
                    AsyncResult.Fail
                } else {
                    AsyncResult.Retry(nextExecution)
                }
            }
            is Result.Succeed<A> -> {
                write(aggregate, cmd, result.events)
                AsyncResult.Success
            }
        }
    }

    private fun write(aggregate: A, cmd: Cmd<A>, events: List<Event<A>>) {
        repository.write(aggregate.aggregateType, cmd.aggregateId, resolveExpectedEventNumber(aggregate.currentEventNumber, cmd.useOptimisticLocking()), events)
    }

    private fun resolveExpectedEventNumber(currentEventNumber: Long, useOptimisticLocking: Boolean): ExpectedEventNumber =
            when (currentEventNumber) {
                -1L -> ExpectedEventNumber.AggregateDoesNotExist
                else -> if (useOptimisticLocking) ExpectedEventNumber.Exact(currentEventNumber) else ExpectedEventNumber.AggregateExists
            }

    private fun readAggregate(cmd: Cmd<A>): A = repository.read(cmd.aggregateId, initAggregate())

    private fun invokeHandler(cmd: Cmd<A>, aggregate: A): Result<A> =
            if (aggregate.currentEventNumber == -1L) {
                initializers.singleOrNull { it.cmdClass == cmd::class }
                        ?.handler
                        ?.invoke(cmd)
                        ?: error("Aggregate ${cmd.aggregateId} does not exist, and no handler found for cmd ${cmd::class.simpleName}")
            } else {
                handlers
                        .singleOrNull { it.cmdClass == cmd::class }
                        ?.handler
                        ?.invoke(aggregate, cmd)
                        ?: error("No handler found for cmd ${cmd::class.simpleName}")
            }

    abstract fun initAggregate(): A

    data class OnCmd<A : Aggregate>(val cmdClass: KClass<Cmd<A>>, val handler: (a: A, c: Cmd<A>) -> Result<A>)
    data class InitOnCmd<A : Aggregate>(val cmdClass: KClass<Cmd<A>>, val handler: (c: Cmd<A>) -> Result<A>)

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

        class Succeed<A : Aggregate>(val events: List<Event<A>>) : Result<A>(null) {
            constructor(event: Event<A>) : this(listOf(event))
        }
    }

    sealed class AsyncResult {
        object Success : AsyncResult()
        object Fail : AsyncResult()
        class Retry(val nextExecution: Instant) : AsyncResult()
    }

}



