package no.ks.kes.lib

import mu.KotlinLogging


private val log = KotlinLogging.logger {}

class SagaManager(eventSubscriber: EventSubscriber, sagaRepository: SagaRepository, sagas: Set<Saga<*>>) {
    private val subscriptions = sagas
            .map { it.getConfiguration() }
            .flatMap { saga ->
                saga.onEvents
                        .map { onEvent ->
                            onEvent.eventClass to { e: EventWrapper<Event<*>> ->
                                sagaRepository.getSagaState(onEvent.correlationId(e), AnnotationUtil.getSerializationId(saga.sagaClass), saga.stateClass)
                                        ?.let {
                                            onEvent.handler.invoke(e, Saga.SagaContext(it))
                                        }
                                        ?.let {
                                            if (it.newState != null || it.commands.isNotEmpty() || it.timeouts.isNotEmpty())
                                                SagaRepository.SagaUpsert.SagaUpdate(
                                                        correlationId = onEvent.correlationId(e),
                                                        serializationId = AnnotationUtil.getSerializationId(saga.sagaClass),
                                                        newState = it.newState,
                                                        commands = it.commands,
                                                        timeouts = it.timeouts.toSet()

                                                )
                                            else
                                                null
                                        }
                            }
                        }
                        .plus(
                                saga.initializer.eventClass to { e: EventWrapper<Event<*>> ->
                                    saga.initializer.handler(e, Saga.InitContext())
                                            .let {
                                                if (it.newState != null)
                                                    SagaRepository.SagaUpsert.SagaInsert(
                                                            correlationId = saga.initializer.correlationId(e),
                                                            serializationId = AnnotationUtil.getSerializationId(saga.sagaClass),
                                                            newState = it.newState!!,
                                                            commands = it.commands
                                                    )
                                                else
                                                    null
                                            }
                                }
                        )
            }
            .groupBy { it.first }

    init {
        eventSubscriber.addSubscriber(
                consumerName = "SagaManager",
                fromEvent = sagaRepository.getCurrentHwm(),
                onEvent = { event ->
                    subscriptions[event.event::class]
                            ?.mapNotNull { it.second.invoke(event) }
                            ?.toSet()
                            ?.apply {
                                sagaRepository.update(event.eventNumber, this)
                            }
                }
        )
    }
}

