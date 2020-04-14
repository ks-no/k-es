package no.ks.kes.lib

import mu.KotlinLogging
import java.util.*
import kotlin.concurrent.schedule
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}
private const val SAGA_SUBSCRIBER = "SagaManager"

object Sagas {

    fun initialize(eventSubscriberFactory: EventSubscriberFactory, sagaRepository: SagaRepository, sagas: Set<Saga<*>>, commandQueue: CommandQueue, pollInterval: Long = 5000) {
        eventSubscriberFactory.createSubscriber(
                subscriber = SAGA_SUBSCRIBER,
                fromEvent = sagaRepository.hwmTracker.getOrInit(SAGA_SUBSCRIBER),
                onEvent = { wrapper ->
                    sagaRepository.transactionally {
                        try {
                            sagaRepository.update(
                                    sagas.mapNotNull {
                                        it.handleEvent(
                                                wrapper = wrapper,
                                                stateProvider = { correlationId: UUID, stateClass: KClass<*> ->
                                                    sagaRepository.getSagaState(correlationId, it.serializationId, stateClass)
                                                })
                                    }.toSet()
                            )
                            sagaRepository.hwmTracker.update(SAGA_SUBSCRIBER, wrapper.eventNumber)
                        } catch (e: Exception) {
                            log.error("An error was encountered while handling incoming event ${wrapper.event::class.simpleName} with sequence number ${wrapper.eventNumber}", e)
                            throw e
                        }
                    }
                }
        )

        Timer("PollingTimeouts", false).schedule(0, pollInterval) {
            sagaRepository.transactionally {
                sagaRepository.getReadyTimeouts()
                        ?.let { timeout ->
                            log.debug { "polled for timeouts, found timeout $timeout" }
                            val matchingSagas = sagas.filter { it.serializationId == timeout.sagaSerializationId }
                            val saga = when {
                                matchingSagas.isEmpty() -> error("Timeout $timeout was triggered, but no sagas matching the serializationId \"${timeout.sagaSerializationId}\" was found. Please check your saga configuration")
                                matchingSagas.size > 1 -> error("Timeout $timeout was triggered, but multiple sagas matching the serializationId \"${timeout.sagaSerializationId}\" was found. Please check your saga configuration")
                                else -> matchingSagas.single()
                            }
                            saga.handleTimeout(
                                    timeout = timeout,
                                    stateProvider = { correlationId: UUID, stateClass: KClass<*> ->
                                        sagaRepository.getSagaState(
                                                correlationId = correlationId,
                                                serializationId = saga.serializationId,
                                                sagaStateClass = stateClass
                                        )
                                    }
                            )?.apply {
                                sagaRepository.update(setOf(this))
                                sagaRepository.deleteTimeout(timeout)
                            }
                        } ?: log.debug { "polled for timeouts, found none" }
            }
        }

        Timer("PollingCommandQueue", false).schedule(0, pollInterval) {
            commandQueue.poll()
        }
    }
}


