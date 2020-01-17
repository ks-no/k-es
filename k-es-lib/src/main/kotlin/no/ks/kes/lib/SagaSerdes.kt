package no.ks.kes.lib

import kotlin.reflect.KClass

interface SagaSerdes {
    fun deserialize(sagaData: ByteArray, sagaStateClass: KClass<Any>): Any
    fun serialize(sagaState: Any): ByteArray
}