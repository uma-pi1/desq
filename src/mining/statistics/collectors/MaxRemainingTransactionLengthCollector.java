package mining.statistics.collectors;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import mining.statistics.data.ProjDbStatData;

public class MaxRemainingTransactionLengthCollector implements DesqProjDbDataCollector<MaxRemainingTransactionLengthCollector, Integer>, 
												Supplier<MaxRemainingTransactionLengthCollector>,
												BiConsumer<MaxRemainingTransactionLengthCollector, ProjDbStatData> {
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
	public BiConsumer<MaxRemainingTransactionLengthCollector, ProjDbStatData> accumulator() {
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
	public void accept(MaxRemainingTransactionLengthCollector t, ProjDbStatData u) {
		if(u.getTransactionId() != this.previousTransactionId) {
			t.maxLength = Integer.max(t.maxLength, u.getTransaction().length - u.getPosition());
			t.previousTransactionId = u.getTransactionId();
		} else {
			// do nothing
		}
	}
}
