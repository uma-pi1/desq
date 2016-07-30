package mining.statistics.collectors;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import com.sun.org.apache.bcel.internal.generic.NEW;

import fst.XFst;
import mining.statistics.data.DesqTransactionData;
import mining.statistics.data.ProjDbStatData;
import tools.FstEdge;
import tools.FstGraph;
import utils.Dictionary;

public class LocalEdgeMaxCycleCollector implements DesqProjDbDataCollector<LocalEdgeMaxCycleCollector, int[]>, 
												Supplier<LocalEdgeMaxCycleCollector>,
												BiConsumer<LocalEdgeMaxCycleCollector, ProjDbStatData> {
	// Data of the accumulator, BiConsumer
	
	public static Int2ObjectOpenHashMap<int[]> globalMaxEdgeCycles;
	int[] localMaxCycle;
	public static final String ID = "LOCAL_EDGE_MAX_REP";
	
	int previousTransactionId;
	private XFst xFst;
	FstGraph graph;
	
	
	public LocalEdgeMaxCycleCollector() {
		previousTransactionId = -1;
	}
	
//	public LocalItemMaxRepetitionCollector(Int2IntOpenHashMap itemFrequencies) {
//		this.localItemFrequencies = itemFrequencies;
//	}
	
	// Collector Method
	@Override
	public Supplier<LocalEdgeMaxCycleCollector> supplier() {
	  return LocalEdgeMaxCycleCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<LocalEdgeMaxCycleCollector, ProjDbStatData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<LocalEdgeMaxCycleCollector> combiner() {
		
		return (acc1,acc2) -> {
//			acc1.localItemFrequencies.putAll(acc2.localItemFrequencies);
			return acc1;
		};
	}

	// Collector Method
	@Override
	public Function<LocalEdgeMaxCycleCollector, int[]> finisher() {
		return (acc) -> acc.localMaxCycle;
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public LocalEdgeMaxCycleCollector get() {
		return new LocalEdgeMaxCycleCollector();
	}

	
	// BiConsumer Method
	@Override
	public void accept(LocalEdgeMaxCycleCollector t, ProjDbStatData u) {
		if(u.getPosition() >= 0 && u.getTransactionId() != previousTransactionId) {
			previousTransactionId  = u.getTransactionId();
			
			if(globalMaxEdgeCycles == null) {
				GlobalEdgeMaxCycleCollector maxEdgeCycleCollector = (GlobalEdgeMaxCycleCollector) u.getGlobalDataCollectors().get(GlobalEdgeMaxCycleCollector.ID);
				@SuppressWarnings("unchecked")
				Function<GlobalEdgeMaxCycleCollector, ArrayList<int[]>> maxEdgeCycleFunc = (Function<GlobalEdgeMaxCycleCollector, ArrayList<int[]>>) u.getGlobalDataCollectors().get(GlobalEdgeMaxCycleCollector.ID).finisher();
				ArrayList<int[]> globalCycleStructure = maxEdgeCycleFunc.apply(maxEdgeCycleCollector);
				globalMaxEdgeCycles = new Int2ObjectOpenHashMap<int[]>(globalCycleStructure.size());
				for (int i = 0; i < globalCycleStructure.size(); i++) {
					globalMaxEdgeCycles.put(i, globalCycleStructure.get(i));
					
				}
				
			}
			
			if(localMaxCycle == null) {
				localMaxCycle = new int[u.getxFst().numStates()];
			}
			
			localMaxCycle[u.getStateFST()] = Integer.max(localMaxCycle[u.getStateFST()], globalMaxEdgeCycles.get(u.getTransactionId())[u.getStateFST()]);
		} else {
			// do nothing
		}
	}
}
