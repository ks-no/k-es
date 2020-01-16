package no.ks.kes.lib

import kotlin.reflect.KClass

object EventUpgrader {
    fun <T : Event<*>> upgradeTo(event: Event<*>, targetClass: KClass<T>): T {
        var upgraded = event
        while (!targetClass.isInstance(upgraded) && upgraded.upgrade() != null)
            upgraded = upgraded.upgrade()!!

        @Suppress("UNCHECKED_CAST")
        if (!targetClass.isInstance(upgraded))
            throw RuntimeException(String.format("Attempted to upgrade event %s to %s, but upgrades where exhausted without arriving at the target class", event::class.simpleName, targetClass.simpleName))
        else
            return upgraded as T
    }

    fun upgrade(event: Event<*>): Event<*> {
        var upgraded = event
        while (upgraded.upgrade() != null) {;
            upgraded = upgraded.upgrade()!!
        }
        return upgraded
    }
}