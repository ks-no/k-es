package no.ks.kes.sagajdbc

import no.ks.kes.lib.CmdHandler
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import javax.sql.DataSource

class JdbcCommandQueue(private val cmdHandler: CmdHandler, val dataSource: DataSource) {

    private val template = NamedParameterJdbcTemplate(dataSource)
    private val transactionManager = DataSourceTransactionManager(dataSource)
    fun poll(){
        TransactionTemplate(transactionManager).execute {
            template.query(
                    """ SELECT FOR UPDATE ${CmdTable.serializationId}, ${CmdTable.data} 
                             FROM ${CmdTable.name} 
                             WHERE ${CmdTable.id} IN (SELECT ${CmdTable.id} ",)
        }

    }


}