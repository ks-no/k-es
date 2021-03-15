package no.ks.kes.lib

import com.google.gson.Gson
import java.util.*

class EventMeta private constructor(val aggregateId: UUID? = null) {

    companion object {
        fun fromJson(json: ByteArray): EventMeta {
            val metaData = String(json)
            if (!metaData.isNullOrEmpty() ) {
                return Gson().fromJson(metaData, EventMeta::class.java)
            } else {
                return EventMeta()
            }
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


