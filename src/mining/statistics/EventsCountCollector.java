package mining.statistics;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class EventsCountCollector implements DesqCountCollector<EventsCountCollector, Integer>, 
												Supplier<EventsCountCollector>,
												BiConsumer<EventsCountCollector, SPMStatisticsData> {
	// Data of the accumulator, BiConsumer 
	int totalEventsCount;
	int previousTransactionId;
	
	// Constructor for the supplier
	public EventsCountCollector() {
		this.previousTransactionId = -1;
	}
	
	// Constructor for the collector
	public EventsCountCollector(int totalEventsCount) {
		this.totalEventsCount = totalEventsCount;
	}
	
	// Collector Method
	@Override
	public Supplier<EventsCountCollector> supplier() {
	  return EventsCountCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<EventsCountCollector, SPMStatisticsData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<EventsCountCollector> combiner() {
		return (acc1,acc2) -> new EventsCountCollector(acc1.totalEventsCount + acc2.totalEventsCount);
	}

	// Collector Method
	@Override
	public Function<EventsCountCollector, Integer> finisher() {
		return (acc) -> new Integer(acc.totalEventsCount);
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public EventsCountCollector get() {
		return new EventsCountCollector(0);
	}

	
	// BiConsumer Method
	@Override
	public void accept(EventsCountCollector t, SPMStatisticsData u) {
		if(u.transactionId != this.previousTransactionId) {
			t.totalEventsCount = t.totalEventsCount + u.transaction.length;
			t.previousTransactionId = u.transactionId;
		} else {
			// do nothing
		}
	}
}
