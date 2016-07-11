package mining.statistics.collectors;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import mining.statistics.data.ProjDbStatData;

public class LocalItemFrequencyCollector implements DesqProjDbDataCollector<LocalItemFrequencyCollector, Int2IntOpenHashMap>, 
												Supplier<LocalItemFrequencyCollector>,
												BiConsumer<LocalItemFrequencyCollector, ProjDbStatData> {
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
	public BiConsumer<LocalItemFrequencyCollector, ProjDbStatData> accumulator() {
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
	public void accept(LocalItemFrequencyCollector t, ProjDbStatData u) {
		if(u.getTransactionId() != this.previousTransactionId && u.getPosition() >= 0) {
			int itemPos = u.getPosition();
			int currentItem;
			while (itemPos < u.getTransaction().length) {
				currentItem = u.getTransaction()[itemPos];
				localItemFrequencies.put(currentItem, localItemFrequencies.get(currentItem) + 1);
				itemPos++;
			}
			t.previousTransactionId = u.getTransactionId();
		} else {
			// do nothing
		}
	}
}
