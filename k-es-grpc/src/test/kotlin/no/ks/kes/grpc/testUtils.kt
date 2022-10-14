package no.ks.kes.grpc

import io.kotest.core.annotation.EnabledCondition
import io.kotest.core.spec.Spec
import kotlin.reflect.KClass

internal class DisableOnArm64: EnabledCondition {
    override fun enabled(kclass: KClass<out Spec>): Boolean = System.getProperty("os.arch") != "aarch64"
}