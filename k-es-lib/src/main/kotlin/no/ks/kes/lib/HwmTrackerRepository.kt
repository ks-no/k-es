package no.ks.kes.lib

interface HwmTrackerRepository {
    fun current(subscriber: String): Long?
    fun getOrInit(subscriber: String): Long
    fun update(subscriber: String, hwm: Long)
}