package no.ks.kes.serdes.jackson

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import no.ks.kes.lib.Aggregate
import no.ks.kes.lib.Cmd
import no.ks.kes.lib.Event
import java.time.Instant
import java.util.*

class SerdesTest : StringSpec() {
    init {
        data class SomeAggregate(val stateInitialized: Boolean, val stateUpdated: Boolean = false) : Aggregate

        @SerializationId("foo")
        data class SomeCmd(override val aggregateId: UUID) : Cmd<SomeAggregate>

        @SerializationId("bar")
        data class SomeOtherCmd(override val aggregateId: UUID) : Cmd<SomeAggregate>

        "test that we can serialize and deserialize commands"{
            val serdes = JacksonCmdSerdes(setOf(SomeCmd::class, SomeOtherCmd::class))

            val someCmd = SomeCmd(UUID.randomUUID())
            val someOtherCmd = SomeOtherCmd(UUID.randomUUID())

            serdes.serialize(someCmd).run { serdes.deserialize(this, "foo") } shouldBe someCmd
            serdes.serialize(someOtherCmd).run { serdes.deserialize(this, "bar") } shouldBe someOtherCmd
        }

        @SerializationId("foo")
        data class SomeEvent(override val aggregateId: UUID) : Event<SomeAggregate>

        @SerializationId("bar")
        data class SomeOtherEvent(override val aggregateId: UUID) : Event<SomeAggregate>

        "test that we can serialize and deserialize events"{
            val serdes = JacksonEventSerdes(setOf(SomeEvent::class, SomeOtherEvent::class))

            val someEvent = SomeEvent(UUID.randomUUID())
            val someOtherEvent = SomeOtherEvent(UUID.randomUUID())

            serdes.serialize(someEvent).run { serdes.deserialize(this, "foo") } shouldBe someEvent
            serdes.serialize(someOtherEvent).run { serdes.deserialize(this, "bar") } shouldBe someOtherEvent
        }

        data class SomeState(val aggregateId: UUID, val timestamp: Instant)

        data class SomeOtherState(val aggregateId: UUID, val timestamp: Instant)

        "test that we can serialize and deserialize saga states"{
            val serdes = JacksonSagaStateSerdes()

            val someState = SomeState(UUID.randomUUID(), Instant.now())
            val someOtherState = SomeOtherState(UUID.randomUUID(), Instant.now())

            serdes.serialize(someState).run { serdes.deserialize(this, SomeState::class) } shouldBe someState
            serdes.serialize(someOtherState).run { serdes.deserialize(this, SomeOtherState::class) } shouldBe someOtherState
        }
    }
}