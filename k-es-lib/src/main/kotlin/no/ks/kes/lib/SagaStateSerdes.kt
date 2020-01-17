package no.ks.kes.lib

import kotlin.reflect.KClass

interface SagaStateSerdes<FORMAT> {
    fun <T: Any> deserialize(sagaData: FORMAT, sagaStateClass: KClass<T>): T
    fun serialize(sagaState: Any): FORMAT
}