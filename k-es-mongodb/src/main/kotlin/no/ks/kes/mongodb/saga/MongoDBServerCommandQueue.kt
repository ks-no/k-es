package no.ks.kes.mongodb.saga

import com.mongodb.client.model.Aggregates
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.Updates
import mu.KotlinLogging
import no.ks.kes.lib.*
import no.ks.kes.mongodb.CmdCollection
import no.ks.kes.mongodb.MongoDBTransactionAwareCollectionFactory
import org.bson.Document
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*


class MongoDBServerCommandQueue(private val factory: MongoDBTransactionAwareCollectionFactory, private val cmdSerdes: CmdSerdes, cmdHandlers: Set<CmdHandler<*>>) : CommandQueue(cmdHandlers) {
    private val cmdCollection get() = factory.getCollection(CmdCollection.name)

    private val transactionTemplate = factory.getTransactionTemplate()

    init {
        factory.initCollection(CmdCollection.name)
    }

    override fun delete(cmdId: Long) {
        cmdCollection.deleteOne(
            Filters.eq(CmdCollection.id, cmdId)
        )
    }

    override fun incrementAndSetError(cmdId: Long, errorId: UUID) {
        cmdCollection.updateOne(
            Filters.eq(CmdCollection.id, cmdId),
            Updates.combine(Updates.set(CmdCollection.error, true), Updates.set(CmdCollection.errorId, errorId), Updates.inc(CmdCollection.retries, 1))
        )
    }

    override fun incrementAndSetNextExecution(cmdId: Long, nextExecution: Instant) {
        cmdCollection.updateOne(
            Filters.eq(CmdCollection.id, cmdId),
            Updates.combine(Updates.set(CmdCollection.nextExecution, DATEFORMAT.format(OffsetDateTime.ofInstant(nextExecution, ZoneOffset.UTC))), Updates.inc(CmdCollection.retries, 1))
        )
    }

    override fun nextCmd(): CmdWrapper<Cmd<*>>? {

        val sort = Aggregates.sort(Sorts.ascending(CmdCollection.id))

        val group = Document("\$group",
            Document("_id", "\$aggregateId")
                .append("data", Document("\$first", "\$data"))
                .append("cmdId", Document("\$first", "\$_id"))
                .append("retries", Document("\$first", "\$retries" ))
                .append("serializationId", Document("\$first", "\$serializationId"))
                .append("error", Document("\$first", "\$error"))
                .append("nextExecution", Document("\$first", "\$nextExecution")))

        val match = Aggregates.match(Filters.and(Filters.lt("nextExecution", DATEFORMAT.format(OffsetDateTime.now(ZoneOffset.UTC))), Filters.eq("error", false)))
        val sample = Aggregates.sample(1)

        return cmdCollection.aggregate(
            listOf(sort, group, match, sample)
        ).map { doc -> CmdWrapper(
            id = doc.getLong("cmdId"),
            cmd = cmdSerdes.deserialize(doc.getString("data").toByteArray(), doc.getString("serializationId")),
            retries = doc.getInteger("retries")
        ) }.singleOrNull()
    }

    override fun transactionally(runnable: () -> Unit) {
        transactionTemplate.execute {
            runnable.invoke()
        }
    }

}