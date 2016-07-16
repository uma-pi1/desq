package mining.statistics.collectors;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import mining.statistics.data.DesqTransactionData;
import utils.Dictionary;

public class GlobalItemMaxRepetitionCollector implements  DesqGlobalDataCollector<GlobalItemMaxRepetitionCollector, int[]>,
													Supplier<GlobalItemMaxRepetitionCollector>,
													BiConsumer<GlobalItemMaxRepetitionCollector, DesqTransactionData> { 
	private int[] repetitionList;
	public static final String ID = "GLOBAL_ITEM_REP_COUNT";

	public GlobalItemMaxRepetitionCollector() {
		this.repetitionList = new int[Dictionary.getInstance().getFlist().length];
	}
		
	@Override
	public Supplier<GlobalItemMaxRepetitionCollector> supplier() {
	  return GlobalItemMaxRepetitionCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<GlobalItemMaxRepetitionCollector, DesqTransactionData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<GlobalItemMaxRepetitionCollector> combiner() {
		// TODO: needs to be implemented correctly for a parallel setting
		return (acc1,acc2) -> acc1;
	}

	// Collector Method
	@Override
	public Function<GlobalItemMaxRepetitionCollector, int[]> finisher() {
		return (acc) -> repetitionList;
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public GlobalItemMaxRepetitionCollector get() {
		return new GlobalItemMaxRepetitionCollector();
	}

	
	// BiConsumer Method
	@Override
	public void accept(GlobalItemMaxRepetitionCollector t, DesqTransactionData u) {
		Int2IntOpenHashMap transactionFreq = new Int2IntOpenHashMap(u.getTransaction().length*2);
		
		for (int i = 0; i < u.getTransaction().length; i++) {
			transactionFreq.addTo(u.getTransaction()[i], 1);
			
			int[] ancestors = Dictionary.getInstance().getAncestors(u.getTransaction()[i]);
			for(int j=0; j<ancestors.length; j++) {
				transactionFreq.addTo(ancestors[j], 1);
			}
		}
		
		for (Iterator<Entry<Integer, Integer>> iterator = transactionFreq.entrySet().iterator(); iterator.hasNext();) {
			Entry<Integer, Integer> entry = (Entry<Integer, Integer>) iterator.next();
			repetitionList[entry.getKey()] = Integer.max(repetitionList[entry.getKey()], entry.getValue());
		}
	}
}
