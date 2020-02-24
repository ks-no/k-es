package no.ks.kes.projectionjdbc

import mu.KotlinLogging
import no.ks.kes.lib.ProjectionRepository
import no.ks.kes.sagajdbc.ProjectionsHwmTable
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

class SqlServerProjectionRepository(dataSource: DataSource) : ProjectionRepository {

    private val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)


    override fun updateHwm(currentEvent: Long) {
        TransactionTemplate(transactionManager).execute {
            try {
                template.update("UPDATE $ProjectionsHwmTable set ${ProjectionsHwmTable.projectionHwm} = :${ProjectionsHwmTable.projectionHwm}", mutableMapOf(ProjectionsHwmTable.projectionHwm to currentEvent))
            } catch (e: Exception) {
                log.error(e) { "An error was encountered while updating hwm for projections, transaction will be rolled back" }
            }
        }
    }

    override fun currentHwm(): Long {
        return template.queryForObject("SELECT ${ProjectionsHwmTable.projectionHwm} from $ProjectionsHwmTable", mutableMapOf<String, Any>(), Long::class.java)
    }
}