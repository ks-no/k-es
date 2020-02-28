package no.ks.kes.jdbc.projection

import mu.KotlinLogging
import no.ks.kes.jdbc.ProjectionsHwmTable
import no.ks.kes.lib.ProjectionRepository
import org.springframework.dao.support.DataAccessUtils
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

class SqlServerProjectionRepository(dataSource: DataSource) : ProjectionRepository {

    private val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)

    override fun updateHwm(currentEvent: Long, consumerName: String) {
        if (hasHwm(consumerName)) {
            updateExistingHwm(currentEvent, consumerName)
        } else {
            insertNewHwm(currentEvent, consumerName)
        }

    }


    override fun currentHwm(consumerName: String): Long {
        return template.queryForList("""SELECT ${ProjectionsHwmTable.projectionHwm} FROM $ProjectionsHwmTable 
                                |WHERE ${ProjectionsHwmTable.consumerName} = :${ProjectionsHwmTable.consumerName}""".trimMargin(),
                mutableMapOf(ProjectionsHwmTable.consumerName to consumerName.trim().toUpperCase()), Long::class.java).let { DataAccessUtils.singleResult(it) }
                ?: 0
    }

    private fun hasHwm(consumerName: String): Boolean {
        return template.queryForObject("""SELECT COUNT(*) FROM $ProjectionsHwmTable 
                                        |WHERE ${ProjectionsHwmTable.consumerName} = :${ProjectionsHwmTable.consumerName}""".trimMargin(),
                mutableMapOf<String, Any>(ProjectionsHwmTable.consumerName to consumerName.toUpperCase()), Long::class.java)
                ?.let { it > 0 } ?: false
    }

    override fun transactionally(runnable: () -> Unit) {
        TransactionTemplate(transactionManager).execute {
            try {
                runnable.invoke()
            } catch (e: Exception) {
                log.error(e) { "An error was encountered while updating hwm for projections, transaction will be rolled back" }
                throw e
            }
        }
    }

    private fun updateExistingHwm(currentEvent: Long, consumerName: String) {
        template.update("""UPDATE $ProjectionsHwmTable set ${ProjectionsHwmTable.projectionHwm} = :${ProjectionsHwmTable.projectionHwm}
                |WHERE ${ProjectionsHwmTable.consumerName}=:${ProjectionsHwmTable.consumerName}""".trimMargin(),
                mutableMapOf(ProjectionsHwmTable.projectionHwm to currentEvent,
                        ProjectionsHwmTable.consumerName to consumerName.trim().toUpperCase()))
    }

    private fun insertNewHwm(currentEvent: Long, consumerName: String) {
        template.update("""INSERT INTO $ProjectionsHwmTable(${ProjectionsHwmTable.projectionHwm}, ${ProjectionsHwmTable.consumerName}) 
            |VALUES (:${ProjectionsHwmTable.projectionHwm}, :${ProjectionsHwmTable.consumerName})""".trimMargin(),
                mutableMapOf(ProjectionsHwmTable.projectionHwm to currentEvent,
                        ProjectionsHwmTable.consumerName to consumerName.toUpperCase()))
    }
}