package no.ks.kes.jdbc.projection

import io.kotlintest.matchers.numerics.shouldBeExactly
import io.kotlintest.matchers.string.shouldStartWith
import io.kotlintest.shouldThrowExactly
import io.kotlintest.specs.StringSpec
import io.mockk.*
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations
import org.springframework.transaction.UnexpectedRollbackException
import org.springframework.transaction.support.TransactionCallback
import org.springframework.transaction.support.TransactionOperations

class SqlServerProjectionRepositoryTest : StringSpec() {

    init {
        "Updates hwm for a consumers that has been previously defined" {
            val namedParameterJdbcOperations: NamedParameterJdbcOperations = mockk()
            val transactionOperations: TransactionOperations = mockk()
            val projectionRepository = SqlServerProjectionRepository(namedParameterJdbcOperations, transactionOperations)
            val updateSql = slot<String>()
            every { namedParameterJdbcOperations.queryForObject(any(), any<Map<String, Any>>(), Long::class.java) } returns 1
            every { namedParameterJdbcOperations.update(capture(updateSql), any<Map<String, Any>>()) } returns 1
            projectionRepository.updateHwm(1L, description().name)
            updateSql.captured shouldStartWith "UPDATE"
            verifyAll {
                namedParameterJdbcOperations.queryForObject(any(), any<Map<String, Any>>(), Long::class.java)
                namedParameterJdbcOperations.update(any(), any<Map<String, Any>>())
            }
            confirmVerified(transactionOperations)
        }

        "Sets hwm for the first time for a consumer" {
            val namedParameterJdbcOperations: NamedParameterJdbcOperations = mockk()
            val transactionOperations: TransactionOperations = mockk()
            val projectionRepository = SqlServerProjectionRepository(namedParameterJdbcOperations, transactionOperations)
            val updateSql = slot<String>()
            every { namedParameterJdbcOperations.queryForObject(any(), any<Map<String, Any>>(), Long::class.java) } returns 0
            every { namedParameterJdbcOperations.update(capture(updateSql), any<Map<String, Any>>()) } returns 1
            projectionRepository.updateHwm(1L, description().name)
            updateSql.captured shouldStartWith "INSERT INTO"
            verifyAll {
                namedParameterJdbcOperations.queryForObject(any(), any<Map<String, Any>>(), Long::class.java)
                namedParameterJdbcOperations.update(any(), any<Map<String, Any>>())
            }
        }

        "Gets the current HWM for a consumer where it has been previously defined" {
            val namedParameterJdbcOperations: NamedParameterJdbcOperations = mockk()
            val transactionOperations: TransactionOperations = mockk()
            val projectionRepository = SqlServerProjectionRepository(namedParameterJdbcOperations, transactionOperations)
            val hwm = 100L
            every { namedParameterJdbcOperations.queryForList(any(), any<Map<String, Any>>(), Long::class.java) } returns listOf(hwm)
            projectionRepository.currentHwm(description().name) shouldBeExactly hwm
            verifyAll {
                namedParameterJdbcOperations.queryForList(any(), any<Map<String, Any>>(), Long::class.java)
            }
            confirmVerified(transactionOperations)
        }

        "Tries to get current HWM when it has not been defined. Should return default value (0)" {
            val namedParameterJdbcOperations: NamedParameterJdbcOperations = mockk()
            val transactionOperations: TransactionOperations = mockk()
            val projectionRepository = SqlServerProjectionRepository(namedParameterJdbcOperations, transactionOperations)
            every { namedParameterJdbcOperations.queryForList(any(), any<Map<String, Any>>(), Long::class.java) } returns emptyList()
            projectionRepository.currentHwm(description().name) shouldBeExactly 0
            verifyAll {
                namedParameterJdbcOperations.queryForList(any(), any<Map<String, Any>>(), Long::class.java)
            }
            confirmVerified(transactionOperations)
        }

        "Executes failing operation within a transaction" {
            val namedParameterJdbcOperations: NamedParameterJdbcOperations = mockk()
            val transactionOperations: TransactionOperations = mockk()
            val projectionRepository = SqlServerProjectionRepository(namedParameterJdbcOperations, transactionOperations)
            every {
                transactionOperations.execute(any<TransactionCallback<Any>>())
            } throws UnexpectedRollbackException("feil")
            shouldThrowExactly<UnexpectedRollbackException> {
                projectionRepository.transactionally {
                    "stuff"
                }
            }
            verifyAll { transactionOperations.execute(any<TransactionCallback<Any>>()) }
            confirmVerified(namedParameterJdbcOperations)
        }

        "Executes operation within a transaction" {
            val namedParameterJdbcOperations: NamedParameterJdbcOperations = mockk()
            val transactionOperations: TransactionOperations = mockk()
            val projectionRepository = SqlServerProjectionRepository(namedParameterJdbcOperations, transactionOperations)
            every {
                transactionOperations.execute(any<TransactionCallback<Any>>())
            } answers {
                args.first()
            }

            projectionRepository.transactionally {
                "stuff"
            }

            verifyAll { transactionOperations.execute(any<TransactionCallback<Any>>()) }
            confirmVerified(namedParameterJdbcOperations)
        }
    }

}