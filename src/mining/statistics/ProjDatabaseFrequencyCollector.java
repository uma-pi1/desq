package mining.statistics;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class ProjDatabaseFrequencyCollector implements Collector<SPMStatisticsData, ProjDatabaseFrequencyCollector, Int2IntOpenHashMap>, 
												Supplier<ProjDatabaseFrequencyCollector>,
												BiConsumer<ProjDatabaseFrequencyCollector, SPMStatisticsData> {
	// Data of the accumulator, BiConsumer 
	Int2IntOpenHashMap localItemFrequencies = new Int2IntOpenHashMap();
	int previousTransactionId;
	
	// Constructor for the supplier
	public ProjDatabaseFrequencyCollector() {
		this.previousTransactionId = -1;
	}
	
	public ProjDatabaseFrequencyCollector(Int2IntOpenHashMap itemFrequencies) {
		this.localItemFrequencies = itemFrequencies;
	}
	
	// Collector Method
	@Override
	public Supplier<ProjDatabaseFrequencyCollector> supplier() {
	  return ProjDatabaseFrequencyCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<ProjDatabaseFrequencyCollector, SPMStatisticsData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<ProjDatabaseFrequencyCollector> combiner() {
		
		return (acc1,acc2) -> {
			acc1.localItemFrequencies.putAll(acc2.localItemFrequencies);
			return acc1;
		};
	}

	// Collector Method
	@Override
	public Function<ProjDatabaseFrequencyCollector, Int2IntOpenHashMap> finisher() {
		return (acc) -> acc.localItemFrequencies;
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public ProjDatabaseFrequencyCollector get() {
		return new ProjDatabaseFrequencyCollector();
	}

	
	// BiConsumer Method
	@Override
	public void accept(ProjDatabaseFrequencyCollector t, SPMStatisticsData u) {
		if(u.transactionId != this.previousTransactionId) {
			int currentItem;
			for (int i = 0; i < u.transaction.length; i++) {
				currentItem = u.transaction[i];
				localItemFrequencies.put(currentItem, localItemFrequencies.get(currentItem) + 1);
			}
			t.previousTransactionId = u.transactionId;
		} else {
			// do nothing
		}
	}
}
