package mining.statistics.collectors;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import mining.statistics.data.DesqTransactionData;
import utils.Dictionary;

public class ItemSupportCollector implements  DesqGlobalDataCollector<ItemSupportCollector, int[]>,
												Supplier<ItemSupportCollector>,
												BiConsumer<ItemSupportCollector, DesqTransactionData> { 
	
	public static final String ID = "ITEMFREQUENCY";
	
	// Constructor for the supplier
	public ItemSupportCollector() {
		// nothing needs to be done if the Dictionary is available
	}
	
	// Collector Method
	@Override
	public Supplier<ItemSupportCollector> supplier() {
	  return ItemSupportCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<ItemSupportCollector, DesqTransactionData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<ItemSupportCollector> combiner() {
		return (acc1,acc2) -> acc1;
	}

	// Collector Method
	@Override
	public Function<ItemSupportCollector, int[]> finisher() {
		return (acc) -> Dictionary.getInstance().getFlist();
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public ItemSupportCollector get() {
		return new ItemSupportCollector();
	}

	
	// BiConsumer Method
	@Override
	public void accept(ItemSupportCollector t, DesqTransactionData u) {
		// do nothing
	}
}
