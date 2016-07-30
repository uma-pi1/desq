package mining.statistics.collectors;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import mining.statistics.data.ProjDbStatData;

public class ProjDatabaseFrequencyCollector implements DesqProjDbDataCollector<ProjDatabaseFrequencyCollector, Int2IntOpenHashMap>, 
												Supplier<ProjDatabaseFrequencyCollector>,
												BiConsumer<ProjDatabaseFrequencyCollector, ProjDbStatData> {
	// Data of the accumulator, BiConsumer
	private static Int2IntOpenHashMap[] transactionItemFrequencies;
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
	public BiConsumer<ProjDatabaseFrequencyCollector, ProjDbStatData> accumulator() {
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
	public void accept(ProjDatabaseFrequencyCollector t, ProjDbStatData u) {
		
		
		
		
		if(u.getTransactionId() != this.previousTransactionId) {

//			if(transactionItemFrequencies == null) {
//				@SuppressWarnings("unchecked")
//				Function<GlobalTransactionsCounter, Integer> eventsCountFunction = (Function<GlobalTransactionsCounter, Integer>) u.getGlobalDataCollectors().get(GlobalTransactionsCounter.ID).finisher();
//				
//				Integer transactions = eventsCountFunction.apply((GlobalTransactionsCounter) u.getGlobalDataCollectors().get(GlobalTransactionsCounter.ID));
//				
//				transactionItemFrequencies = new Int2IntOpenHashMap[transactions];
//			}
//			
//			if(transactionItemFrequencies[u.getTransactionId()] == null) {
			int currentItem;
//			transactionItemFrequencies[u.getTransactionId()] = new Int2IntOpenHashMap();
			for (int i = 0; i < u.getTransaction().length; i++) {
				currentItem = u.getTransaction()[i];
				localItemFrequencies.addTo(currentItem, 1);
//				transactionItemFrequencies[u.getTransactionId()].addTo(currentItem, 1);
			}
				
//			} else {
//				System.out.println("CACHE");
//				for (ObjectIterator<Entry<Integer, Integer>> iterator = transactionItemFrequencies[u.getTransactionId()].entrySet().iterator(); iterator.hasNext();) {
//					Entry<Integer, Integer> entry = (Entry<Integer, Integer>) iterator.next();
//					localItemFrequencies.addTo(entry.getKey(), entry.getValue());
//				}
//				
//			}
			t.previousTransactionId = u.getTransactionId();
		} else {
			// do nothing
		}
	}
}
