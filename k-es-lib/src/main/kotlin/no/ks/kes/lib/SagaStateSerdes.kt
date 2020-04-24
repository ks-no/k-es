package no.ks.kes.lib

import kotlin.reflect.KClass

interface SagaStateSerdes {
    fun <T : Any> deserialize(sagaData: ByteArray, sagaStateClass: KClass<T>): T
    fun serialize(sagaState: Any): ByteArray
}