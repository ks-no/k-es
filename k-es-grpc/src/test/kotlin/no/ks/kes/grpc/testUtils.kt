package no.ks.kes.grpc

import io.kotest.core.annotation.Condition
import io.kotest.core.spec.Spec
import kotlin.reflect.KClass

internal class DisableOnArm64 : Condition {
    override fun evaluate(kclass: KClass<out Spec>): Boolean = System.getProperty("os.arch") != "aarch64"
}