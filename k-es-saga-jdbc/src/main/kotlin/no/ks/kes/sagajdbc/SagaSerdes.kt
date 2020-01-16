package no.ks.kes.sagajdbc

import kotlin.reflect.KClass

interface SagaSerdes {
    fun deserialize(sagaData: ByteArray, sagaStateClass: KClass<Any>): Any
    fun serialize(sagaState: Any): ByteArray
}