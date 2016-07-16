package mining.statistics.collectors;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.lucene.util.FixedBitSet;

import com.zaxxer.sparsebits.SparseBitSet;

import mining.statistics.data.DesqTransactionData;
import mining.statistics.data.ProjDbStatData;
import utils.Dictionary;

public class LocalItemOccurranceIndicator implements DesqProjDbDataCollector<LocalItemOccurranceIndicator, SparseBitSet>, 
												Supplier<LocalItemOccurranceIndicator>,
												BiConsumer<LocalItemOccurranceIndicator, ProjDbStatData> {
	// Data of the accumulator, BiConsumer 
	public static final String ID = "LOCAL_ITEM_OCC_INDICATOR";
	int previousTransactionId;
	ArrayList<SparseBitSet> globalItemOccList;
	SparseBitSet localItemOccList = new SparseBitSet(Dictionary.getInstance().getFlist().length);
	int items = Dictionary.getInstance().getFlist().length;
	
	public LocalItemOccurranceIndicator() {
		this.previousTransactionId = -1;
	}
	
//	public LocalItemMaxRepetitionCollector(Int2IntOpenHashMap itemFrequencies) {
//		this.localItemFrequencies = itemFrequencies;
//	}
	
	// Collector Method
	@Override
	public Supplier<LocalItemOccurranceIndicator> supplier() {
	  return LocalItemOccurranceIndicator::new;
	}

	// Collector Method
	@Override
	public BiConsumer<LocalItemOccurranceIndicator, ProjDbStatData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<LocalItemOccurranceIndicator> combiner() {
		
		return (acc1,acc2) -> {
//			acc1.localItemFrequencies.putAll(acc2.localItemFrequencies);
			return acc1;
		};
	}

	// Collector Method
	@Override
	public Function<LocalItemOccurranceIndicator, SparseBitSet> finisher() {
		return (acc) -> acc.localItemOccList;
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public LocalItemOccurranceIndicator get() {
		return new LocalItemOccurranceIndicator();
	}

	
	// BiConsumer Method
	@Override
	public void accept(LocalItemOccurranceIndicator t, ProjDbStatData u) {
		
		if(u.getTransactionId() != this.previousTransactionId && u.getPosition() >= 0) {
			
//			if(globalItemOccList == null) {
//				GlobalItemOccurrenceCollector itemOccIndicator = (GlobalItemOccurrenceCollector) u.getGlobalDataCollectors().get(GlobalItemOccurrenceCollector.ID);
//				@SuppressWarnings("unchecked")
//				Function<GlobalItemOccurrenceCollector,  ArrayList<FixedBitSet>> itemIndicatorFunc = (Function<GlobalItemOccurrenceCollector, ArrayList<FixedBitSet>>) u.getGlobalDataCollectors().get(GlobalItemOccurrenceCollector.ID).finisher();
//				globalItemOccList = itemIndicatorFunc.apply(itemOccIndicator);
//			}
			
			int itemPos = u.getPosition();
			int currentItem;
			while (itemPos < u.getTransaction().length) {
				currentItem = u.getTransaction()[itemPos];
				localItemOccList.set((currentItem-items)*-1);
				itemPos++;
			}
		} else {
			// do nothing
		}
	}
}
