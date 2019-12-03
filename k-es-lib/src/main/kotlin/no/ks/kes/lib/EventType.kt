package no.ks.kes.lib

import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy

@Retention(RetentionPolicy.RUNTIME)
annotation class EventType(val value: String)