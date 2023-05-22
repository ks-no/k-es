package no.ks.kes.grpc

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk

class GrpcSubscriptionListenerTest : StringSpec({
    "Test too slow exception verification" {
        GrpcSubscriptionListener.isTooSlowException(RuntimeException()) shouldBe false

        val statusRuntimeException = mockk<io.grpc.StatusRuntimeException>()
        every { statusRuntimeException.message } returns "ABORTED: Operation timed out: Consumer too slow to handle event while live. Client resubscription required."
        GrpcSubscriptionListener.isTooSlowException(statusRuntimeException) shouldBe true


        val statusRuntimeExceptionUnknown = mockk<io.grpc.StatusRuntimeException>()
        every { statusRuntimeException.message } returns "ABORTED: Operation failed: unknown. Client resubscription required."
        GrpcSubscriptionListener.isTooSlowException(statusRuntimeException) shouldBe false
    }
})