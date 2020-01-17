package no.ks.kes.sagajdbc

import no.ks.kes.lib.SagaStateSerdes
import kotlin.reflect.KClass

class JdbcSagaStateSerdes : SagaStateSerdes<String> {
    override fun <T : Any> deserialize(sagaData: String, sagaStateClass: KClass<T>): T {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun serialize(sagaState: Any): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}