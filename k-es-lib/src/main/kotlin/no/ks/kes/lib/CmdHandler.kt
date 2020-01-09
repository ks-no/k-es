package no.ks.kes.lib

class CmdHandler(private val writer: EventWriter, private val reader: AggregateReader) {
    fun <A: Aggregate> handle(cmd: Cmd<A>): A {
        val aggregate: A = reader.read(cmd.aggregateId, cmd.initAggregate())
        val events = cmd.execute(aggregate)
        writer.write(aggregate.aggregateType, cmd.aggregateId, aggregate.currentEventNumber, events, cmd.useOptimisticLocking());
        return events.fold(
                aggregate)
        { a, e -> a.applyEvent(e, Long.MIN_VALUE) }
    }

}