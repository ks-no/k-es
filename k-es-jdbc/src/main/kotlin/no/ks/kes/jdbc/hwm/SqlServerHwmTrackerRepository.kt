package no.ks.kes.jdbc.hwm

import mu.KotlinLogging
import no.ks.kes.jdbc.HwmTable
import no.ks.kes.lib.HwmTrackerRepository
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

private val log = KotlinLogging.logger {}

class SqlServerHwmTrackerRepository(private val template: NamedParameterJdbcTemplate, private val schema: String? = null, private val initialHwm: Long) : HwmTrackerRepository {

    override fun current(subscriber: String): Long? =
            template.queryForList(
            """
                        SELECT ${HwmTable.hwm} FROM ${HwmTable.qualifiedName(schema)} 
                        WHERE ${HwmTable.subscriber} = :${HwmTable.subscriber}  
                        """,
            mapOf(HwmTable.subscriber to subscriber),
            Long::class.java)
                    .singleOrNull()

    override fun getOrInit(subscriber: String): Long =
            current(subscriber) ?: initHwm(subscriber)
                    .also { log.info { "no hwm found for subscriber $subscriber, initializing subscriber at $initialHwm" } }

    override fun update(subscriber: String, hwm: Long) {
        template.update(
                """
                    UPDATE ${HwmTable.qualifiedName(schema)}  SET ${HwmTable.hwm} = :${HwmTable.hwm}
                    WHERE ${HwmTable.subscriber} = :${HwmTable.subscriber}
                 """,
                mapOf(
                        HwmTable.subscriber to subscriber,
                        HwmTable.hwm to hwm
                ))
                .also { if (it != 1) error("Error updating hwm for $subscriber, expected 1 row changed on update, but $it was changed") }
    }

    private fun initHwm(subscriber: String): Long {
        template.update(
                """ 
                    INSERT INTO ${HwmTable.qualifiedName(schema)}  (${HwmTable.subscriber}, ${HwmTable.hwm})
                    VALUES (:${HwmTable.subscriber}, :initialHwm) 
                """,
                        mapOf(
                                HwmTable.subscriber to subscriber,
                                "initialHwm" to initialHwm
                        ))
                .also { if (it != 1) error("Error inserting new hwm for $subscriber, expected 1 row changed on insert, but $it was inserted") }
        return initialHwm
    }
}

