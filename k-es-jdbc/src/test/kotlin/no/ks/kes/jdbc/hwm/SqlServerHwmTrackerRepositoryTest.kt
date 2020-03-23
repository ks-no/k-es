package no.ks.kes.jdbc.hwm

import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import kotlin.random.Random

class SqlServerHwmTrackerRepositoryTest : StringSpec() {
    init {

        "Test that a subscriber hwm is created if one does not exist" {
            val template = mockk<NamedParameterJdbcTemplate>()
                    .apply {
                        every { queryForList(any(), ofType<Map<String, *>>(), Long::class.java) } returns emptyList<Long>()
                        every { update(any(), ofType<Map<String, *>>()) } returns 1
                    }
            SqlServerHwmTrackerRepository(template).getOrInit("some-subscriber")
            verify { template.update(any(), ofType<Map<String, *>>()) }
        }

        "Test that a subscriber hwm is returned if it exists" {
            val hwm = Random.nextLong()
            val template = mockk<NamedParameterJdbcTemplate>()
                    .apply {
                        every { queryForList(any(), ofType<Map<String, *>>(), Long::class.java) } returns listOf(hwm)
                    }
            SqlServerHwmTrackerRepository(template).getOrInit("some-subscriber") shouldBe hwm
        }

        "Test that an exception is thrown if a hwm-update operation does not change any rows" {
            val hwm = Random.nextLong()
            val template = mockk<NamedParameterJdbcTemplate>()
                    .apply {
                        every { update(any(), ofType<Map<String, *>>()) } returns 0
                    }
            shouldThrow<IllegalStateException> { SqlServerHwmTrackerRepository(template).update("some-subscriber", Random.nextLong()) }
        }

        "Test that no exception is thrown if a hwm-update operation changes exactly one row" {
            val hwm = Random.nextLong()
            val template = mockk<NamedParameterJdbcTemplate>()
                    .apply {
                        every { update(any(), ofType<Map<String, *>>()) } returns 1
                    }
            SqlServerHwmTrackerRepository(template).update("some-subscriber", Random.nextLong())
            verify { template.update(any(), ofType<Map<String, *>>()) }
        }

    }
}