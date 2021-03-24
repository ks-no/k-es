package no.ks.kes.test.example

import no.ks.kes.lib.*
import no.ks.kes.serdes.jackson.JacksonCmdSerdes
import no.ks.kes.serdes.jackson.JacksonEventSerdes
import java.time.Duration
import java.time.Instant
import java.util.*

/**
 * All example code are considered to be part of the documentation and may be changed at any time without past notice
 */
private val LOG = mu.KotlinLogging.logger {  }

data class EngineProperties(val id: UUID, val running: Boolean, val startCount: Int = 0) : Aggregate

const val ENGINE_AGGREGATE_TYPE = "Engine"
const val SAGA_SERILIZATION_ID = "EngineSaga"

data class EngineSagaState(val aggregateId: UUID, val startInitiated: Boolean, val stoppedBySaga: Boolean = false)

object EngineSaga : Saga<EngineSagaState>(EngineSagaState::class, SAGA_SERILIZATION_ID) {
    init {
        init { e: Events.Created, aggregateId: UUID ->
            LOG.debug { "Saga created: $aggregateId" }
            dispatch(Cmds.Start(aggregateId = aggregateId))
            setState(EngineSagaState(aggregateId = aggregateId, startInitiated = true))
        }

        apply { e: Events.Stopped, aggregateId: UUID ->
            LOG.debug { "Saga handles Stopped: $aggregateId" }
            setState(state.copy(startInitiated = false))
        }

        timeout({ e: Events.Started, aggregateId: UUID -> aggregateId }, { Instant.now().plus(Duration.ofSeconds(5L)) }) {
            LOG.debug { "Saga timed: ${state.aggregateId}" }
            if (state.startInitiated) {
                setState(state.copy(stoppedBySaga = true))
                dispatch(Cmds.Stop(state.aggregateId))
            }
        }
    }
}

object Engine : AggregateConfiguration<EngineProperties>(ENGINE_AGGREGATE_TYPE) {

    init {
        init { e: Events.Created, aggregateId: UUID ->
            EngineProperties(id = aggregateId, running = false)
        }

        apply<Events.Started> { copy(running = true, startCount = startCount + 1) }

        apply<Events.Stopped> { copy(running = false) }
    }

}

class EngineCmdHandler(repository: AggregateRepository) : CmdHandler<EngineProperties>(repository, Engine) {
    init {
        init<Cmds.Create> {
            LOG.debug { "Create command: ${it.aggregateId}" }
            Result.Succeed(
                Event( eventData = Events.Created(it.aggregateId), aggregateId = it.aggregateId))
        }

        apply<Cmds.Start> {
            LOG.debug { "Tries to start" }
            if (running) {
                Result.Succeed()
            } else {
                Result.Succeed(
                    Event( eventData = Events.Started(it.aggregateId), aggregateId = it.aggregateId)
                )
            }
        }

        apply<Cmds.Stop> {
            if (running) {
                Result.Succeed(
                    Event( eventData = Events.Stopped(it.aggregateId), aggregateId = it.aggregateId)
                )
            } else {
                Result.Fail(RuntimeException("Can not stop engine that has already been stopped"))
            }
        }

        apply<Cmds.Check> {
            Result.Succeed()
        }
    }
}

abstract class EngineCommand(override val aggregateId: UUID) : Cmd<EngineProperties>

object Cmds {

    val all = setOf(Create::class, Start::class, Stop::class, Check::class)
    val serdes = JacksonCmdSerdes(all)

    @SerializationId("Created")
    data class Create(override val aggregateId: UUID) : EngineCommand(aggregateId)

    @SerializationId("Start")
    data class Start(override val aggregateId: UUID) : EngineCommand(aggregateId)

    @SerializationId("Stop")
    data class Stop(override val aggregateId: UUID) : EngineCommand(aggregateId)

    @SerializationId("Check")
    data class Check(override val aggregateId: UUID) : EngineCommand(aggregateId)
}

abstract class EngineEvent(val description: String) : EventData<EngineProperties> {
    val timestamp: Instant = Instant.now()
}

object Events {

    val all = setOf(Created::class, Started::class, Stopped::class)
    val serdes = JacksonEventSerdes(all)

    @SerializationId("Created")
    data class Created(val aggregateId: UUID) : EngineEvent("Created engine with id $aggregateId")

    @SerializationId("Started")
    data class Started(val aggregateId: UUID) : EngineEvent("Engine $aggregateId started")

    @SerializationId("Stopped")
    data class Stopped(val aggregateId: UUID) : EngineEvent("Engine $aggregateId stopped")
}

class EnginesProjection: Projection() {
    private val enginesDefined = mutableSetOf<UUID>()
    private val runningEngines = mutableSetOf<UUID>()
    private val stoppedEngines = mutableSetOf<UUID>()
    val all: Set<UUID>
        get() = enginesDefined.toSet()

    val allRunning: Set<UUID>
        get() = runningEngines.toSet()

    val allStopped: Set<UUID>
        get() = stoppedEngines.toSet()

    init {
        on<Events.Created> {
            enginesDefined.plusAssign(it.aggregateId)
        }
        on<Events.Started> {
            runningEngines.plusAssign(it.aggregateId)
            stoppedEngines.minusAssign(it.aggregateId)
        }
        on<Events.Stopped> {
            stoppedEngines.plusAssign(it.aggregateId)
            runningEngines.minusAssign(it.aggregateId)
        }
    }
}