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

public class LocalMaxItemCollector implements DesqProjDbDataCollector<LocalMaxItemCollector, SparseBitSet>, 
												Supplier<LocalMaxItemCollector>,
												BiConsumer<LocalMaxItemCollector, ProjDbStatData> {
	// Data of the accumulator, BiConsumer 
	public static final String ID = "LOCAL_ITEM_OCC_INDICATOR";
	int previousTransactionId;
	ArrayList<FixedBitSet> globalItemOccList;
	SparseBitSet localItemOccList = new SparseBitSet(Dictionary.getInstance().getFlist().length);
	int items = Dictionary.getInstance().getFlist().length;
	int minStartPosition;
	
	public LocalMaxItemCollector() {
		this.previousTransactionId = -1;
		this.minStartPosition = -1;
	}
	
//	public LocalItemMaxRepetitionCollector(Int2IntOpenHashMap itemFrequencies) {
//		this.localItemFrequencies = itemFrequencies;
//	}
	
	public void clear() {
		localItemOccList = null;
	}
	
	// Collector Method
	@Override
	public Supplier<LocalMaxItemCollector> supplier() {
	  return LocalMaxItemCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<LocalMaxItemCollector, ProjDbStatData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<LocalMaxItemCollector> combiner() {
		
		return (acc1,acc2) -> {
//			acc1.localItemFrequencies.putAll(acc2.localItemFrequencies);
			return acc1;
		};
	}

	// Collector Method
	@Override
	public Function<LocalMaxItemCollector, SparseBitSet> finisher() {
		return (acc) -> acc.localItemOccList;
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public LocalMaxItemCollector get() {
		return new LocalMaxItemCollector();
	}

	
	// BiConsumer Method
	@Override
	public void accept(LocalMaxItemCollector t, ProjDbStatData u) {
		if(u.getPosition() >= 0) {
			if(u.getTransactionId() != this.previousTransactionId) {
				previousTransactionId = u.getTransactionId();
				minStartPosition = u.getPosition();
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
				 
				if(u.getPosition() < this.minStartPosition) {
					int itemPos = u.getPosition();
					int currentItem;
					while (itemPos < this.minStartPosition) {
						currentItem = u.getTransaction()[itemPos];
						localItemOccList.set((currentItem-items)*-1);
						itemPos++;
					}
					this.minStartPosition = u.getPosition();
				}
			}
		}
	}
}
