package no.ks.kes.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.kotest.assertions.asClue
import io.kotest.assertions.throwables.shouldThrowExactly
import io.kotest.assertions.timing.eventually
import io.kotest.core.spec.style.StringSpec
import io.kotest.extensions.testcontainers.perSpec
import io.kotest.matchers.shouldBe
import no.ks.kes.jdbc.hwm.SqlServerHwmTrackerRepository
import no.ks.kes.jdbc.projection.SqlServerProjectionRepository
import no.ks.kes.jdbc.saga.SqlServerCommandQueue
import no.ks.kes.jdbc.saga.SqlServerSagaRepository
import no.ks.kes.lib.Sagas
import no.ks.kes.serdes.jackson.JacksonSagaStateSerdes
import no.ks.kes.test.example.*
import no.ks.kes.test.withKes
import org.junit.jupiter.api.fail
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.jdbc.datasource.SingleConnectionDataSource
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.MSSQLServerContainer
import org.testcontainers.utility.DockerImageName
import java.sql.DriverManager
import java.time.Duration
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

private const val INTIAL_HWM = -1L
private val LOG = mu.KotlinLogging.logger { }

@ExperimentalTime
class MSSQLContainerTest : StringSpec() {
    private val mssqlContainer = KMSSQLContainer()

    init {
        listener(mssqlContainer.perSpec())
        "Testing HWM backed by SQLServer" {
            val subscriber = testCase.name.testName
            mssqlContainer.createConnection().use { connection ->
                NamedParameterJdbcTemplate(SingleConnectionDataSource(connection, true)).run {
                    val sqlServerHwmTrackerRepository = SqlServerHwmTrackerRepository(template = this, initialHwm = INTIAL_HWM)

                    sqlServerHwmTrackerRepository.current(subscriber) shouldBe null
                    val initialHwm = sqlServerHwmTrackerRepository.getOrInit(subscriber)
                    initialHwm shouldBe INTIAL_HWM
                    sqlServerHwmTrackerRepository.update(subscriber, (initialHwm + 1L))
                    sqlServerHwmTrackerRepository.current(subscriber) shouldBe (initialHwm + 1L)

                }
            }

        }

        "Testing ProjectionRepository backed by SQLServer" {
            val subscriber = testCase.name.testName
            mssqlContainer.createConnection().use { connection ->
                val dataSource = SingleConnectionDataSource(connection, true)
                val projectionRepository = SqlServerProjectionRepository(dataSource)
                projectionRepository.hwmTracker.current(subscriber) shouldBe null
                shouldThrowExactly<RuntimeException> {
                    projectionRepository.transactionally {
                        projectionRepository.hwmTracker.getOrInit(subscriber) shouldBe INTIAL_HWM
                        throw RuntimeException("Stuff")
                    }
                }
                // HWM setup should be rolled back
                projectionRepository.hwmTracker.current(subscriber) shouldBe null
                dataSource.destroy()
            }
        }

        "Test using SagaRepository and CommandQueue backed by SQLServer" {
            HikariDataSource(HikariConfig()
                .apply {
                    jdbcUrl = mssqlContainer.jdbcUrl
                    username = mssqlContainer.username
                    password = mssqlContainer.password
                    maximumPoolSize = 5
                }
            ).use { dataSource ->
                withKes(
                    eventSerdes = Events.serdes,
                    cmdSerdes = Cmds.serdes
                ) { kesTest ->
                    val sagaRepository = SqlServerSagaRepository(
                        dataSource = dataSource,
                        sagaStateSerdes = JacksonSagaStateSerdes(),
                        cmdSerdes = kesTest.cmdSerdes
                    )
                    val cmdHandler = EngineCmdHandler(repository = kesTest.aggregateRepository)
                    val commandQueue = SqlServerCommandQueue(dataSource, kesTest.cmdSerdes, cmdHandlers = setOf(cmdHandler))
                    Sagas.initialize(eventSubscriberFactory = kesTest.subscriberFactory,
                        sagaRepository = sagaRepository,
                        sagas = setOf(EngineSaga),
                        commandQueue = commandQueue,
                        pollInterval = 1000,
                        onClose = {
                            fail(it)
                        }
                    )
                    val aggregateId = UUID.randomUUID()
                    TransactionTemplate(DataSourceTransactionManager(dataSource)).execute {
                        cmdHandler.handle(Cmds.Create(aggregateId))
                    }
                    eventually(10.seconds) {
                        cmdHandler.handle(Cmds.Check(aggregateId)).asClue {
                            it.startCount shouldBe 1
                            it.running shouldBe false
                        }
                        sagaRepository.getSagaState(aggregateId, SAGA_SERILIZATION_ID, EngineSagaState::class)?.asClue {
                            it.stoppedBySaga shouldBe true
                        } ?: fail { "Ingen saga state funnet for aggregat $aggregateId" }

                    }
                }

            }

        }
    }


}

private val IMAGE_NAME = DockerImageName.parse("mcr.microsoft.com/mssql/server").withTag("2017-latest")

class KMSSQLContainer(password: String = "123K-es-password") : MSSQLServerContainer<KMSSQLContainer>(IMAGE_NAME) {

    init {
        LOG.debug { "Setting up Docker container \"${IMAGE_NAME.asCanonicalNameString()}\"" }
        acceptLicense()
            .withPassword(password)
            .withInitScript("init/kes-ddl.sql")
            .withStartupTimeout(Duration.ofMinutes(2L))
            .withUrlParam("encrypt", "false")
    }

    fun createConnection() = DriverManager.getConnection(jdbcUrl, username, password)


}

