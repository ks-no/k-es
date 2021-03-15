package no.ks.kes.lib

import com.google.gson.Gson
import java.util.*

class EventMeta private constructor(val aggregateId: UUID?) {

    companion object {
        fun fromJson(json: ByteArray): EventMeta {
            return Gson().fromJson(String(json), EventMeta::class.java)
        }
    }

    data class Builder(
        var aggregateId: UUID? = null
    ) {

        fun aggregateId(aggregateId: UUID) = apply { this.aggregateId = aggregateId }
        fun build() = EventMeta(aggregateId)
    }

    fun serialize(): String {
        return Gson().toJson(this)
    }
}


