package mining.statistics.collectors;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongBitSet;

import mining.statistics.data.DesqTransactionData;
import utils.Dictionary;

public class GlobalItemOccurrenceCollector implements  DesqGlobalDataCollector<GlobalItemOccurrenceCollector, ArrayList<LongBitSet>>,
													Supplier<GlobalItemOccurrenceCollector>,
													BiConsumer<GlobalItemOccurrenceCollector, DesqTransactionData> { 
	
	private ArrayList<LongBitSet> itemOccPerTransaction;
	public static final String ID = "GLOBAL_ITEM_OCCURRANCE_INDICATOR";

	public GlobalItemOccurrenceCollector() {
		this.itemOccPerTransaction = new ArrayList<LongBitSet>();
	}
		
	@Override
	public Supplier<GlobalItemOccurrenceCollector> supplier() {
	  return GlobalItemOccurrenceCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<GlobalItemOccurrenceCollector, DesqTransactionData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<GlobalItemOccurrenceCollector> combiner() {
		// TODO: needs to be implemented correctly for a parallel setting
		return (acc1,acc2) -> acc1;
	}

	// Collector Method
	@Override
	public Function<GlobalItemOccurrenceCollector, ArrayList<LongBitSet>> finisher() {
		return (acc) -> acc.itemOccPerTransaction;
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public GlobalItemOccurrenceCollector get() {
		return new GlobalItemOccurrenceCollector();
	}

	
	// BiConsumer Method
	@Override
	public void accept(GlobalItemOccurrenceCollector t, DesqTransactionData u) {
		LongBitSet transBitSet = new LongBitSet(Dictionary.getInstance().getFlist().length);
	
		for (int i = 0; i < u.getTransaction().length; i++) {
			transBitSet.set(u.getTransaction()[i]);
			
			int[] ancestors = Dictionary.getInstance().getAncestors(u.getTransaction()[i]);
			for(int j=0; j<ancestors.length; j++) {
				transBitSet.set(ancestors[j]);
			}
		}
		
		itemOccPerTransaction.add(transBitSet);
	}
}
