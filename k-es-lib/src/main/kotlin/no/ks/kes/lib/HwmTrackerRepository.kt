package no.ks.kes.lib

interface HwmTrackerRepository {
    fun getOrInit(subscriber: String): Long
    fun update(subscriber: String, hwm: Long)
}