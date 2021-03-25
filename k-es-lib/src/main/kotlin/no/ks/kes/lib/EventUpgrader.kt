package no.ks.kes.lib

import kotlin.reflect.KClass

object EventUpgrader {
    fun <T : EventData<*>> upgradeTo(eventData: EventData<*>, targetClass: KClass<T>): T {
        var upgraded = eventData
        while (!targetClass.isInstance(upgraded) && upgraded.upgrade() != null)
            upgraded = upgraded.upgrade()!!

        @Suppress("UNCHECKED_CAST")
        if (!targetClass.isInstance(upgraded))
            throw RuntimeException(String.format("Attempted to upgrade event %s to %s, but upgrades where exhausted without arriving at the target class", eventData::class.simpleName, targetClass.simpleName))
        else
            return upgraded as T
    }

    fun upgrade(eventData: EventData<*>): EventData<*> {
        var upgraded = eventData
        while (upgraded.upgrade() != null) {;
            upgraded = upgraded.upgrade()!!
        }
        return upgraded
    }
}