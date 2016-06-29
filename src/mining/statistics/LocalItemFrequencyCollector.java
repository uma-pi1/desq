package mining.statistics;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class LocalItemFrequencyCollector implements Collector<SPMStatisticsData, LocalItemFrequencyCollector, Int2IntOpenHashMap>, 
												Supplier<LocalItemFrequencyCollector>,
												BiConsumer<LocalItemFrequencyCollector, SPMStatisticsData> {
	// Data of the accumulator, BiConsumer 
	Int2IntOpenHashMap localItemFrequencies = new Int2IntOpenHashMap();
	int previousTransactionId;
	
	// Constructor for the supplier
	public LocalItemFrequencyCollector() {
		this.previousTransactionId = -1;
	}
	
	public LocalItemFrequencyCollector(Int2IntOpenHashMap itemFrequencies) {
		this.localItemFrequencies = itemFrequencies;
	}
	
	// Collector Method
	@Override
	public Supplier<LocalItemFrequencyCollector> supplier() {
	  return LocalItemFrequencyCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<LocalItemFrequencyCollector, SPMStatisticsData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<LocalItemFrequencyCollector> combiner() {
		
		return (acc1,acc2) -> {
			acc1.localItemFrequencies.putAll(acc2.localItemFrequencies);
			return acc1;
		};
	}

	// Collector Method
	@Override
	public Function<LocalItemFrequencyCollector, Int2IntOpenHashMap> finisher() {
		return (acc) -> acc.localItemFrequencies;
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public LocalItemFrequencyCollector get() {
		return new LocalItemFrequencyCollector();
	}

	
	// BiConsumer Method
	@Override
	public void accept(LocalItemFrequencyCollector t, SPMStatisticsData u) {
		if(u.transactionId != this.previousTransactionId) {
			int itemPos = u.position;
			int currentItem;
			while (itemPos < u.transaction.length) {
				currentItem = u.transaction[itemPos];
				localItemFrequencies.put(currentItem, localItemFrequencies.get(currentItem) + 1);
				itemPos++;
			}
			t.previousTransactionId = u.transactionId;
		} else {
			// do nothing
		}
	}
}
