package mining.statistics.collectors;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import mining.statistics.data.ProjDbStatData;

public class EventsCountCollector implements DesqProjDbDataCollector<EventsCountCollector, Integer>, 
												Supplier<EventsCountCollector>,
												BiConsumer<EventsCountCollector, ProjDbStatData> {
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
	public BiConsumer<EventsCountCollector, ProjDbStatData> accumulator() {
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
	public void accept(EventsCountCollector t, ProjDbStatData u) {
		if(u.getTransactionId() != this.previousTransactionId) {
			t.totalEventsCount = t.totalEventsCount + u.getTransaction().length;
			t.previousTransactionId = u.getTransactionId();
		} else {
			// do nothing
		}
	}
}
