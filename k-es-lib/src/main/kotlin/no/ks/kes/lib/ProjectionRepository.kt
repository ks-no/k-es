package no.ks.kes.lib

interface ProjectionRepository {

    fun updateHwm(currentEvent: Long)
    fun currentHwm(): Long
}