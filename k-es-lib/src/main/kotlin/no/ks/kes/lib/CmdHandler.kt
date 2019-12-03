package no.ks.kes.lib

class CmdHandler<E : Event, AGGREGATE : Aggregate<E>>(private val writer: EventWriter, private val reader: AggregateReader) {
    fun handle(cmd: Cmd<E, AGGREGATE>): AGGREGATE {
        val aggregate: AGGREGATE = reader.read(cmd.aggregateId, cmd.initAggregate())
        val events = cmd.execute(aggregate)
        writer.write(aggregate.aggregateType, cmd.aggregateId, aggregate.currentEventNumber, events, cmd.useOptimisticLocking());
        return events.fold(
                aggregate)
        { a, e -> a.applyEvent(e, Long.MIN_VALUE) }
    }

}