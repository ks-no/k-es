package no.ks.kes.jdbc.projection

import mu.KotlinLogging
import no.ks.kes.jdbc.hwm.SqlServerHwmTrackerRepository
import no.ks.kes.lib.ProjectionRepository
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

class SqlServerProjectionRepository(dataSource: DataSource, initialHwm: Long = -1, schema: String? = null) : ProjectionRepository {
    private val transactionManager =  TransactionTemplate(DataSourceTransactionManager(dataSource))
    override val hwmTracker = SqlServerHwmTrackerRepository(
            template = NamedParameterJdbcTemplate(dataSource),
            schema = schema,
            initialHwm = initialHwm
    )

    override fun transactionally(runnable: () -> Unit) {
        transactionManager.execute {
            try {
                runnable.invoke()
            } catch (e: Exception) {
                log.error(e) { "An error was encountered while updating hwm for projections, transaction will be rolled back" }
                throw e
            }
        }
    }
}