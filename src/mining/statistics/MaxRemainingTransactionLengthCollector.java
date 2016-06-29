package mining.statistics;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class MaxRemainingTransactionLengthCollector implements Collector<SPMStatisticsData, MaxRemainingTransactionLengthCollector, Integer>, 
												Supplier<MaxRemainingTransactionLengthCollector>,
												BiConsumer<MaxRemainingTransactionLengthCollector, SPMStatisticsData> {
	// Data of the accumulator, BiConsumer 
	int maxLength;
	int previousTransactionId;
	
	// Constructor for the supplier
	public MaxRemainingTransactionLengthCollector() {
		this.previousTransactionId = -1;
	}
	
	// Constructor for the collector
	public MaxRemainingTransactionLengthCollector(int maxLength) {
		this.maxLength = maxLength;
	}
	
	// Collector Method
	@Override
	public Supplier<MaxRemainingTransactionLengthCollector> supplier() {
	  return MaxRemainingTransactionLengthCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<MaxRemainingTransactionLengthCollector, SPMStatisticsData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<MaxRemainingTransactionLengthCollector> combiner() {
		return (acc1,acc2) -> new MaxRemainingTransactionLengthCollector(Integer.max(acc1.maxLength, acc2.maxLength));
	}

	// Collector Method
	@Override
	public Function<MaxRemainingTransactionLengthCollector, Integer> finisher() {
		return (acc) -> new Integer(acc.maxLength);
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public MaxRemainingTransactionLengthCollector get() {
		return new MaxRemainingTransactionLengthCollector(0);
	}

	
	// BiConsumer Method
	@Override
	public void accept(MaxRemainingTransactionLengthCollector t, SPMStatisticsData u) {
		if(u.transactionId != this.previousTransactionId) {
			t.maxLength = Integer.max(t.maxLength, u.transaction.length - u.position);
			t.previousTransactionId = u.transactionId;
		} else {
			// do nothing
		}
	}
}
