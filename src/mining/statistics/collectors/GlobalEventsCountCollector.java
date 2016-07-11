package mining.statistics.collectors;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import mining.statistics.data.DesqTransactionData;

public class GlobalEventsCountCollector implements DesqGlobalDataCollector<GlobalEventsCountCollector, Integer>, 
												Supplier<GlobalEventsCountCollector>,
												BiConsumer<GlobalEventsCountCollector, DesqTransactionData> {
	// Data of the accumulator, BiConsumer 
	int totalEventsCount;
	int previousTransactionId;
	
	// Constructor for the supplier
	public GlobalEventsCountCollector() {
		this.previousTransactionId = -1;
	}
	
	// Constructor for the collector
	public GlobalEventsCountCollector(int totalEventsCount) {
		this.totalEventsCount = totalEventsCount;
	}
	
	// Collector Method
	@Override
	public Supplier<GlobalEventsCountCollector> supplier() {
	  return GlobalEventsCountCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<GlobalEventsCountCollector, DesqTransactionData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<GlobalEventsCountCollector> combiner() {
		return (acc1,acc2) -> new GlobalEventsCountCollector(acc1.totalEventsCount + acc2.totalEventsCount);
	}

	// Collector Method
	@Override
	public Function<GlobalEventsCountCollector, Integer> finisher() {
		return (acc) -> new Integer(acc.totalEventsCount);
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public GlobalEventsCountCollector get() {
		return new GlobalEventsCountCollector(0);
	}

	
	// BiConsumer Method
	@Override
	public void accept(GlobalEventsCountCollector t, DesqTransactionData u) {
		if(u.getTransactionId() != this.previousTransactionId) {
			t.totalEventsCount = t.totalEventsCount + u.getTransaction().length;
			t.previousTransactionId = u.getTransactionId();
		} else {
			// do nothing
		}
	}
}
