package mining.statistics.collectors;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Arrays;
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

import fst.XFst;
import mining.statistics.data.DesqTransactionData;
import mining.statistics.data.ProjDbStatData;
import tools.FstEdge;
import tools.FstGraph;
import utils.Dictionary;

public class GlobalEdgeMaxCycleCollector implements DesqGlobalDataCollector<GlobalEdgeMaxCycleCollector, ArrayList<int[]>>, 
												Supplier<GlobalEdgeMaxCycleCollector>,
												BiConsumer<GlobalEdgeMaxCycleCollector, DesqTransactionData> {
	// Data of the accumulator, BiConsumer 
	private Int2IntOpenHashMap repetitionList;
	public static final String ID = "TRANS_MAX_CYCLE_COLLECTOR";
	int previousTransactionId;
	private XFst xFst;
	FstGraph graph;
	Object[] fstCycles;
	ArrayList<int[]> maxLengthPerState;
	
	
	public GlobalEdgeMaxCycleCollector() {
		this.repetitionList = new Int2IntOpenHashMap();
		this.previousTransactionId = -1;
	}
	
//	public LocalItemMaxRepetitionCollector(Int2IntOpenHashMap itemFrequencies) {
//		this.localItemFrequencies = itemFrequencies;
//	}
	
	// Collector Method
	@Override
	public Supplier<GlobalEdgeMaxCycleCollector> supplier() {
	  return GlobalEdgeMaxCycleCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<GlobalEdgeMaxCycleCollector, DesqTransactionData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<GlobalEdgeMaxCycleCollector> combiner() {
		
		return (acc1,acc2) -> {
//			acc1.localItemFrequencies.putAll(acc2.localItemFrequencies);
			return acc1;
		};
	}

	// Collector Method
	@Override
	public Function<GlobalEdgeMaxCycleCollector, ArrayList<int[]>> finisher() {
		return (acc) -> acc.maxLengthPerState;
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public GlobalEdgeMaxCycleCollector get() {
		return new GlobalEdgeMaxCycleCollector();
	}

	
	// BiConsumer Method
	@Override
	public void accept(GlobalEdgeMaxCycleCollector t, DesqTransactionData u) {
		if(u.getTransactionId() != this.previousTransactionId) {
			if(graph == null) {
				graph = u.getxFst().convertToFstGraph();
				this.xFst = u.getxFst();
			}
			
			if(maxLengthPerState == null) {
				maxLengthPerState = new ArrayList<int[]>();
			}
			
			if(fstCycles == null) {
				List<FstEdge[]> allCycles = graph.getAllCycles(false);
				fstCycles = new Object[allCycles.size()];
				for(int i = 0; i<allCycles.size(); i++) {
					for(int cycleEdge = 0; cycleEdge < allCycles.get(i).length; cycleEdge++) {
						if(cycleEdge == 0) {
							System.out.print(allCycles.get(i)[cycleEdge].getFromState());
						}
						System.out.print(" -> " + allCycles.get(i)[cycleEdge].getToState());
					}
					System.out.println("");
					fstCycles[i] = allCycles.get(i);
				}
			}
			
			Object[] transactionCycles = new Object[fstCycles.length];
			int[] transactionCycleCount = new int[fstCycles.length];
			System.arraycopy(fstCycles, 0, transactionCycles, 0, fstCycles.length);
			
			boolean cycleBroken = false;
			
			// count max cycles iteration per transaction
			for (int pos = 0; pos < u.getTransaction().length; pos++) {
				for(int cycle = 0; cycle < transactionCycles.length; cycle++) {
					if(cycle == 0) {
						// skip first cycle
						continue;
					}
					int internalPos = pos;
					int positionCycleCount = 0;
					FstEdge[] transitions = (FstEdge[]) transactionCycles[cycle];
					while(internalPos < u.getTransaction().length) {
						
						for(int transitionCnt = 0; transitionCnt < transitions.length; transitionCnt++) {
							if(internalPos < u.getTransaction().length) {
								FstEdge transition = transitions[transitionCnt];
								if(xFst.canStep(u.getTransaction()[internalPos], transition.getFromState(), transition.getTransitionId())) {
									internalPos++;
								} else {
	//								transactionCycles[cycle] = null;
									cycleBroken = true;
									break;
								}
							} else {
								cycleBroken = true;
								break;
							}
						}
						
						if(cycleBroken == true) {
							cycleBroken = false;
							break;
						}
						positionCycleCount++;
					}
					
					transactionCycleCount[cycle] = Integer.max(transactionCycleCount[cycle], positionCycleCount);
				}
			}
			
			int[] stateLengths = new int[xFst.numStates()];
			for(int i = 0; i<xFst.numStates(); i++) {
				stateLengths[i] = graph.getMaxTransitionsToFinalState(i, transactionCycleCount);
				maxLengthPerState.add(stateLengths);
//				System.out.println(Arrays.toString(stateLengths));
			}
			
			
//			System.out.println(Arrays.toString(transactionCycleCount));

//				for(FstEdge edge : edges) {
//					if(edge.isPartOfCylce() == true) {
//						xFst.canStep(u.getTransaction()[i], u.getStateFST(), edge.getTransitionId());
//						edgeFreq.addTo(edge.getTransitionId(), 1);
//					}
//				}
//			}
//			
//			for (Iterator<Entry<Integer, Integer>> iterator = edgeFreq.entrySet().iterator(); iterator.hasNext();) {
//				Entry<Integer, Integer> entry = (Entry<Integer, Integer>) iterator.next();
//				Integer currentValue = repetitionList.get(entry.getKey());
//				if(currentValue != null) {
//					repetitionList.put(entry.getKey().intValue(), Integer.max(currentValue, entry.getValue()));
//				} else {
//					repetitionList.put(entry.getKey(),entry.getValue());
//				}
//			}
		} else {
			// do nothing
		}
	}
}
