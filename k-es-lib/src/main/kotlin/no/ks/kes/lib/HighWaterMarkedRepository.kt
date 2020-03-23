package no.ks.kes.lib

interface HighWaterMarkedRepository {
    val hwmTracker: HwmTrackerRepository
}