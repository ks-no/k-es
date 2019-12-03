package no.ks.kes.lib

import mu.KotlinLogging
import kotlin.reflect.KClass

private val log = KotlinLogging.logger {}
abstract class Projection {
 private val projectors: MutableMap<String, (EventWrapper<*>) -> Any?> = mutableMapOf()

    open fun onLive(){
        return
    }

    protected infix fun <T: Event> KClass<T>.project(projector: (T) -> Any?){
        this projectAsWrapper {projector.invoke(it.event)}
    }

    protected infix fun <T: Event> KClass<T>.projectAsWrapper(projector: (EventWrapper<T>) -> Any?){
        @Suppress("UNCHECKED_CAST")
        projectors[EventUtil.getEventType(this)] = {e ->
            log.info("Event ${EventUtil.getEventType(this)} on aggregate ${e.event.aggregateId} " +
                    "received by projection ${this@Projection::class.simpleName}")
            projector.invoke(e as EventWrapper<T>)
        }
    }

    fun accept(wrapper: EventWrapper<*>){
        projectors[EventUtil.getEventType(wrapper.event::class)]
                ?.invoke(wrapper)
    }
}