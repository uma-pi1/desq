package mining.statistics.collectors;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import mining.statistics.data.ProjDbStatData;

public class FstStateItemCollector implements DesqProjDbDataCollector<FstStateItemCollector, HashSet<Integer>>, 
												Supplier<FstStateItemCollector>,
												BiConsumer<FstStateItemCollector, ProjDbStatData> {
	
	public static final String ID = "FST_STATES";
	
	// Data of the accumulator, BiConsumer 
	HashSet<Integer> fstStates = new HashSet<Integer>();
	
	// Constructor for the supplier
	public FstStateItemCollector() {
	}
	
	// Constructor for the collector
	public FstStateItemCollector(HashSet<Integer> fstStates) {
		this.fstStates = fstStates;
	}
	
	// Collector Method
	@Override
	public Supplier<FstStateItemCollector> supplier() {
	  return FstStateItemCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<FstStateItemCollector, ProjDbStatData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<FstStateItemCollector> combiner() {
		return (acc1,acc2) -> {
			acc1.fstStates.addAll(acc2.fstStates);
			return acc1;
		};
	}

	// Collector Method
	@Override
	public Function<FstStateItemCollector, HashSet<Integer>> finisher() {
		return (acc) -> acc.fstStates;
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public FstStateItemCollector get() {
		return new FstStateItemCollector();
	}

	
	// BiConsumer Method
	@Override
	public void accept(FstStateItemCollector t, ProjDbStatData u) {
		t.fstStates.add(u.getStateFST());
	}
}
