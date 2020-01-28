package no.ks.kes.sagajdbc

import mu.KotlinLogging
import no.ks.kes.lib.SagaManager
import no.ks.kes.lib.SagaRepository
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

class SqlServerSagaTimeoutManager(dataSource: DataSource, private val sagaManager: SagaManager) {
    private val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)

    fun poll() {
        TransactionTemplate(transactionManager).executeWithoutResult {
            getReadyTimeouts()
                    ?.also {
                        log.info { "polled for timeouts, found timeout with sagaSerializationId: \"${it.sagaSerializationId}\", sagaCorrelationId: \"${it.sagaCorrelationId}\", timeoutId: \"${it.timeoutId}\"" }
                    }
                    ?.apply {
                        sagaManager.onTimeoutReady(sagaSerializationId, sagaCorrelationId, timeoutId)
                    } ?: log.info { "polled for timeouts, found none" }
        }
    }

    fun getReadyTimeouts(): SagaRepository.Timeout? {
        return template.query("""
            SELECT TOP 1 ${TimeoutTable.sagaSerializationId}, ${TimeoutTable.sagaCorrelationId}, ${TimeoutTable.timeoutId} 
            FROM $TimeoutTable 
            WITH (UPDLOCK, NOWAIT)
            WHERE ${TimeoutTable.error} = 0
            AND ${TimeoutTable.timeout}  < CURRENT_TIMESTAMP 
        """) { r, i ->
            SagaRepository.Timeout(
                    sagaSerializationId = r.getString(TimeoutTable.sagaSerializationId),
                    sagaCorrelationId = UUID.fromString(r.getString(TimeoutTable.sagaCorrelationId)),
                    timeoutId = r.getString(TimeoutTable.timeoutId)
            )
        }.singleOrNull()
    }
}