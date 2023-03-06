package no.ks.kes.lib

import mu.KotlinLogging
import kotlin.system.exitProcess

private val log = KotlinLogging.logger {}

object Projections {
    fun <S: EventSubscription> initialize(
        eventSubscriberFactory: EventSubscriberFactory<S>,
        projections: Set<Projection>,
        projectionRepository: ProjectionRepository,
        hwmId: String,
        onClose: (Exception) -> Unit = defaultOnCloseHandler
    ): S {
        val validatedProjectionConfigurations = projections.map { projection -> projection.getConfiguration { eventSubscriberFactory.getSerializationId(it) } }

        return eventSubscriberFactory.createSubscriber(
                hwmId = hwmId,
                onEvent = { wrapper ->
                    projectionRepository.transactionally {
                        validatedProjectionConfigurations.forEach {
                            it.accept(wrapper)
                        }
                        projectionRepository.hwmTracker.update(hwmId, wrapper.eventNumber)
                    }
                },
                fromEvent = projectionRepository.hwmTracker.getOrInit(hwmId),
                onClose = { onClose.invoke(it) },
                onLive = { projections.forEach { it.onLive() } }
        )
    }
}

val defaultOnCloseHandler = { exception: Exception ->
    log.error(exception) { "Event subscription was closed. Shutting down." }
    exitProcess(1)
}