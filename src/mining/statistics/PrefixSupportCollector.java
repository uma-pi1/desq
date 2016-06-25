package mining.statistics;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class PrefixSupportCollector implements Collector<SPMStatisticsData, PrefixSupportCollector, Integer>, 
												Supplier<PrefixSupportCollector>,
												BiConsumer<PrefixSupportCollector, SPMStatisticsData> {
	// Data of the accumulator, BiConsumer 
	int support;
	int previousTransactionId;
	
	// Constructor for the supplier
	public PrefixSupportCollector() {
		this.previousTransactionId = -1;
	}
	
	// Constructor for the collector
	public PrefixSupportCollector(int support) {
		this.support = support;
	}
	
	// Collector Method
	@Override
	public Supplier<PrefixSupportCollector> supplier() {
	  return PrefixSupportCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<PrefixSupportCollector, SPMStatisticsData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<PrefixSupportCollector> combiner() {
		return (acc1,acc2) -> new PrefixSupportCollector(acc1.support + acc2.support);
	}

	// Collector Method
	@Override
	public Function<PrefixSupportCollector, Integer> finisher() {
		return (acc) -> new Integer(acc.support);
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public PrefixSupportCollector get() {
		return new PrefixSupportCollector(0);
	}

	
	// BiConsumer Method
	@Override
	public void accept(PrefixSupportCollector t, SPMStatisticsData u) {
		if(u.transactionId != this.previousTransactionId) {
			t.support = t.support + 1;
			t.previousTransactionId = u.transactionId;
		} else {
			// do nothing
		}
	}
}
