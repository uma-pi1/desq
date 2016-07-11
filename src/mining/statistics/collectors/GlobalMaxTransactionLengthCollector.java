package mining.statistics.collectors;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import mining.statistics.data.DesqTransactionData;
import mining.statistics.data.ProjDbStatData;

public class GlobalMaxTransactionLengthCollector implements DesqGlobalDataCollector<GlobalMaxTransactionLengthCollector, Integer>, 
												Supplier<GlobalMaxTransactionLengthCollector>,
												BiConsumer<GlobalMaxTransactionLengthCollector, DesqTransactionData> {
	// Data of the accumulator, BiConsumer 
	int maxLength;
	int previousTransactionId;
	public static final String ID = "GLOBAL_MAX_TRANSACTION_LENGTH";
	
	// Constructor for the supplier
	public GlobalMaxTransactionLengthCollector() {
		this.previousTransactionId = -1;
	}
	
	// Constructor for the collector
	public GlobalMaxTransactionLengthCollector(int maxLength) {
		this.maxLength = maxLength;
	}
	
	// Collector Method
	@Override
	public Supplier<GlobalMaxTransactionLengthCollector> supplier() {
	  return GlobalMaxTransactionLengthCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<GlobalMaxTransactionLengthCollector, DesqTransactionData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<GlobalMaxTransactionLengthCollector> combiner() {
		return (acc1,acc2) -> new GlobalMaxTransactionLengthCollector(Integer.max(acc1.maxLength, acc2.maxLength));
	}

	// Collector Method
	@Override
	public Function<GlobalMaxTransactionLengthCollector, Integer> finisher() {
		return (acc) -> new Integer(acc.maxLength);
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public GlobalMaxTransactionLengthCollector get() {
		return new GlobalMaxTransactionLengthCollector(0);
	}

	
	// BiConsumer Method
	@Override
	public void accept(GlobalMaxTransactionLengthCollector t, DesqTransactionData u) {
		if(u.getTransactionId() != this.previousTransactionId) {
			t.maxLength = Integer.max(t.maxLength, u.getTransaction().length);
			t.previousTransactionId = u.getTransactionId();
		} else {
			// do nothing
		}
	}
}
