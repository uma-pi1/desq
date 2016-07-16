package mining.scores;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.function.Function;

import org.apache.lucene.util.FixedBitSet;

import com.zaxxer.sparsebits.SparseBitSet;

import fst.OutputLabel;
import fst.XFst;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.collectors.FstStateItemCollector;
import mining.statistics.collectors.GlobalEdgeMaxCycleCollector;
import mining.statistics.collectors.GlobalEventsCountCollector;
import mining.statistics.collectors.GlobalItemMaxRepetitionCollector;
import mining.statistics.collectors.GlobalItemOccurrenceCollector;
import mining.statistics.collectors.GlobalMaxTransactionLengthCollector;
import mining.statistics.collectors.ItemSupportCollector;
import mining.statistics.collectors.LocalEdgeMaxCycleCollector;
import mining.statistics.collectors.LocalItemOccurranceIndicator;
import mining.statistics.collectors.MaxRemainingTransactionLengthCollector;
import tools.FstEdge;
import tools.FstGraph;
import utils.Dictionary;

public class InformationGainScore extends DesqBaseScore {
	FstGraph fstGraph;
	XFst xFst;
	HashMap<Integer, SortedSet<Double>> fstStateList;
	double[] gainList;
	Object[] stateItems;
	double[][] maxExtensionValuesPerState;
	
	public InformationGainScore(XFst xFst) {
		super(xFst);
		this.xFst = xFst;
		this.fstGraph = xFst.convertToFstGraph();
		this.stateItems = new Object[xFst.numStates()];
	}

	@Override
	public HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?,?>, ?>> getProjDbCollectors() {
		HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> collectors = new HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>();
//		collectors.put("LOCAL_ITEM_FREQUENCIES", new LocalItemFrequencyCollector());
//		collectors.put(LocalItemOccurranceIndicator.ID, new LocalItemOccurranceIndicator());
//		collectors.put("PREFIXSUPPORT", new PrefixSupportCollector());
		
//		collectors.put("FST_STATES", new FstStateItemCollector());
//		collectors.put(LocalEdgeMaxCycleCollector.ID, new LocalEdgeMaxCycleCollector());
//		collectors.put("MAX_REMAIN_TRANSACTION_LENGTH", new MaxRemainingTransactionLengthCollector());

		return collectors;
	}
	
	@Override
	public HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> getGlobalDataCollectors() {
		HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> collectors = new HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>>();
		collectors.put("TOTAL_EVENT_COUNT", new GlobalEventsCountCollector());
		collectors.put(ItemSupportCollector.ID, new ItemSupportCollector());
		collectors.put(GlobalItemMaxRepetitionCollector.ID, new GlobalItemMaxRepetitionCollector());
		collectors.put(GlobalMaxTransactionLengthCollector.ID, new GlobalMaxTransactionLengthCollector());
//		collectors.put(GlobalItemOccurrenceCollector.ID, new GlobalItemOccurrenceCollector());
		collectors.put(GlobalEdgeMaxCycleCollector.ID, new GlobalEdgeMaxCycleCollector());
		
		return collectors;
	}
	
	@Override
	public double getScoreBySequence(int[] sequence, 
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors) {		
		
		if(gainList == null) {
			createInformationGainList(globalDataCollectors,Dictionary.getInstance().getFlist());
		}
		if(sequence != null) {
			double totalInformationGain = 0;
			
			for (int i = 0; i < sequence.length; i++) {
				totalInformationGain = totalInformationGain + gainList[sequence[i]];
			}
//			System.out.println(totalInformationGain);
			return totalInformationGain;
		} else {
			return 0;
		}
		
	}
	
	public double getScoreByProjDb(int[] sequence, 
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>> finalStateProjDbCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>>[] prefixProjDbCollectors) {

		return getScoreBySequence(sequence, globalDataCollectors);	
	}
	
/*
 * (non-Javadoc)
 * this function works based on global data for DESQ-DFS
 */

//	public double getMaxScoreByPrefix(int[] prefix,  
//			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
//			HashMap<String,? extends DesqProjDbDataCollector<?,?>>[] prefixProjDbCollectors) {
////		return Double.MAX_VALUE;
//		FstStateItemCollector sup = (FstStateItemCollector) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(FstStateItemCollector.ID);
//		@SuppressWarnings("unchecked")
//		Function<FstStateItemCollector, HashSet<Integer>> func = (Function<FstStateItemCollector, HashSet<Integer>>) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(FstStateItemCollector.ID).finisher();
//		
//		HashSet<Integer> fstStates = func.apply(sup);
//		
//		GlobalItemMaxRepetitionCollector maxRepCollector = (GlobalItemMaxRepetitionCollector) globalDataCollectors.get(GlobalItemMaxRepetitionCollector.ID);
//		@SuppressWarnings("unchecked")
//		Function<GlobalItemMaxRepetitionCollector, int[]> maxRepFunc = (Function<GlobalItemMaxRepetitionCollector, int[]>) globalDataCollectors.get(GlobalItemMaxRepetitionCollector.ID).finisher();
//		int[] maxRepetitionList = maxRepFunc.apply(maxRepCollector);
//		
//		LocalEdgeMaxCycleCollector maxEdgeCycleCollector = (LocalEdgeMaxCycleCollector) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(LocalEdgeMaxCycleCollector.ID);
//		@SuppressWarnings("unchecked")
//		Function<LocalEdgeMaxCycleCollector, int[]> maxEdgeCycleFunc = (Function<LocalEdgeMaxCycleCollector, int[]>) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(LocalEdgeMaxCycleCollector.ID).finisher();
//		int[] maxEdgeCycles = maxEdgeCycleFunc.apply(maxEdgeCycleCollector);
//
////		MaxRemainingTransactionLengthCollector maxRemainLenghtCollector = (MaxRemainingTransactionLengthCollector) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(MaxRemainingTransactionLengthCollector.ID);
////		@SuppressWarnings("unchecked")
////		Function<MaxRemainingTransactionLengthCollector, Integer> maxRemainLenghtFunc = (Function<MaxRemainingTransactionLengthCollector, Integer>) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(MaxRemainingTransactionLengthCollector.ID).finisher();		
////		int maxTransactionLenght = maxRemainLenghtFunc.apply(maxRemainLenghtCollector); 
//
//		if(fstStateList == null) {
//			createStateItems(globalDataCollectors);
//		}
//		
//		if(maxExtensionValuesPerState == null) {
//			GlobalMaxTransactionLengthCollector maxTransLengthCollector = (GlobalMaxTransactionLengthCollector) globalDataCollectors.get(GlobalMaxTransactionLengthCollector.ID);
//			@SuppressWarnings("unchecked")
//			Function<GlobalMaxTransactionLengthCollector, Integer> maxTransLengthFunc = (Function<GlobalMaxTransactionLengthCollector, Integer>) globalDataCollectors.get(GlobalMaxTransactionLengthCollector.ID).finisher();
//			int globalMaxTransLength = maxTransLengthFunc.apply(maxTransLengthCollector);
//			
//			maxExtensionValuesPerState = new double[xFst.numStates()][globalMaxTransLength];
//			double initValue = -1;
//			for(int i = 0; i<xFst.numStates(); i++) {
//				Arrays.fill(maxExtensionValuesPerState[i], initValue);
//			}
//		}
//		
//		
//		double maxInformationGain = getScoreBySequence(prefix, globalDataCollectors);
//		double initValue = -1;
//		double maxScore = 0;
//		for (Iterator<Integer> iterator = fstStates.iterator(); iterator.hasNext();) {
//			Integer fstState = (Integer) iterator.next();
//			int maxLength = maxEdgeCycles[fstState];
//			
//			if(maxExtensionValuesPerState[fstState][maxLength] == initValue) {
//				double maxAdditionalInformationGain = getMaxInformationGain(prefix, maxLength, fstState, maxRepetitionList, globalDataCollectors);
//				maxScore = Double.max(maxInformationGain + maxAdditionalInformationGain, maxScore);
//				maxExtensionValuesPerState[fstState][maxLength] = maxAdditionalInformationGain;
//			} else {
//				maxScore = Double.max(maxInformationGain + getMaxInformationGain(prefix, maxLength, fstState, maxRepetitionList, globalDataCollectors), maxScore);
//			}
//		}
//		
////		System.out.println(maxScore);
//		return maxScore;
//	}

	
	/*
	 * (non-Javadoc)
	 * this function works based on local data for DESQ-DFS
	 */
	public double getMaxScoreByPrefix(int[] prefix,  
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>>[] prefixProjDbCollectors) {
//		return Double.MAX_VALUE;
		FstStateItemCollector sup = (FstStateItemCollector) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(FstStateItemCollector.ID);
		@SuppressWarnings("unchecked")
		Function<FstStateItemCollector, HashSet<Integer>> func = (Function<FstStateItemCollector, HashSet<Integer>>) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(FstStateItemCollector.ID).finisher();
		
		HashSet<Integer> fstStates = func.apply(sup);
		
//		GlobalItemMaxRepetitionCollector maxRepCollector = (GlobalItemMaxRepetitionCollector) globalDataCollectors.get(GlobalItemMaxRepetitionCollector.ID);
//		@SuppressWarnings("unchecked")
//		Function<GlobalItemMaxRepetitionCollector, int[]> maxRepFunc = (Function<GlobalItemMaxRepetitionCollector, int[]>) globalDataCollectors.get(GlobalItemMaxRepetitionCollector.ID).finisher();
//		int[] maxRepetitionList = maxRepFunc.apply(maxRepCollector);

//		MaxRemainingTransactionLengthCollector maxRemainLenghtCollector = (MaxRemainingTransactionLengthCollector) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(MaxRemainingTransactionLengthCollector.ID);
//		@SuppressWarnings("unchecked")
//		Function<MaxRemainingTransactionLengthCollector, Integer> maxRemainLenghtFunc = (Function<MaxRemainingTransactionLengthCollector, Integer>) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(MaxRemainingTransactionLengthCollector.ID).finisher();		
//		int maxTransactionLenght = maxRemainLenghtFunc.apply(maxRemainLenghtCollector); 

		LocalItemOccurranceIndicator localItemOccIndicator = (LocalItemOccurranceIndicator) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(LocalItemOccurranceIndicator.ID);
		@SuppressWarnings("unchecked")
		Function<LocalItemOccurranceIndicator, SparseBitSet> localItemOccFunc = (Function<LocalItemOccurranceIndicator, SparseBitSet>) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(LocalItemOccurranceIndicator.ID).finisher();		
		SparseBitSet localItemOcc = localItemOccFunc.apply(localItemOccIndicator); 	
		
		LocalEdgeMaxCycleCollector maxEdgeCycleCollector = (LocalEdgeMaxCycleCollector) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(LocalEdgeMaxCycleCollector.ID);
		@SuppressWarnings("unchecked")
		Function<LocalEdgeMaxCycleCollector, int[]> maxEdgeCycleFunc = (Function<LocalEdgeMaxCycleCollector, int[]>) prefixProjDbCollectors[prefixProjDbCollectors.length-1].get(LocalEdgeMaxCycleCollector.ID).finisher();
		int[] maxEdgeCycles = maxEdgeCycleFunc.apply(maxEdgeCycleCollector);

		if(fstStateList == null) {
			createStateItems(globalDataCollectors);
		}
		
		double maxScore = 0;
		for (Iterator<Integer> iterator = fstStates.iterator(); iterator.hasNext();) {
			Integer fstState = (Integer) iterator.next();
			int maxLength = maxEdgeCycles[fstState];
			
			maxScore = Double.max(maxScore, getMaxInformationGain(prefix, maxLength, fstState, localItemOcc));
		}
		
		return maxScore;
	}
	
	@Override
	public double getMaxScoreByPrefix(
			int[] prefix,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors,
			int[] transaction,
			int transactionId,
			int position,
			int fstState) {
//		return Double.MAX_VALUE;
		
		if(fstStateList == null) {
			createStateItems(globalDataCollectors);
		}
		
		GlobalItemMaxRepetitionCollector sup = (GlobalItemMaxRepetitionCollector) globalDataCollectors.get(GlobalItemMaxRepetitionCollector.ID);
		@SuppressWarnings("unchecked")
		Function<GlobalItemMaxRepetitionCollector, int[]> func = (Function<GlobalItemMaxRepetitionCollector, int[]>) globalDataCollectors.get(GlobalItemMaxRepetitionCollector.ID).finisher();
		int[] maxRepetitionList = func.apply(sup);
		
		GlobalEdgeMaxCycleCollector maxEdgeCycleCollector = (GlobalEdgeMaxCycleCollector) globalDataCollectors.get(GlobalEdgeMaxCycleCollector.ID);
		@SuppressWarnings("unchecked")
		Function<GlobalEdgeMaxCycleCollector, ArrayList<int[]>> maxEdgeCycleFunc = (Function<GlobalEdgeMaxCycleCollector, ArrayList<int[]>>) globalDataCollectors.get(GlobalEdgeMaxCycleCollector.ID).finisher();
		ArrayList<int[]> maxEdgeCycles = maxEdgeCycleFunc.apply(maxEdgeCycleCollector);
		
		if(maxExtensionValuesPerState == null) {
			GlobalMaxTransactionLengthCollector maxTransLengthCollector = (GlobalMaxTransactionLengthCollector) globalDataCollectors.get(GlobalMaxTransactionLengthCollector.ID);
			@SuppressWarnings("unchecked")
			Function<GlobalMaxTransactionLengthCollector, Integer> maxTransLengthFunc = (Function<GlobalMaxTransactionLengthCollector, Integer>) globalDataCollectors.get(GlobalMaxTransactionLengthCollector.ID).finisher();
			int globalMaxTransLength = maxTransLengthFunc.apply(maxTransLengthCollector);
			
			maxExtensionValuesPerState = new double[xFst.numStates()][globalMaxTransLength];
			double initValue = -1;
			for(int i = 0; i<xFst.numStates(); i++) {
				Arrays.fill(maxExtensionValuesPerState[i], initValue);
			}
			
		}
		
		int maxLength = transaction.length - position;
		maxLength = Integer.min(maxLength,maxEdgeCycles.get(transactionId)[fstState]);
		double maxInformationGain = getScoreBySequence(prefix, globalDataCollectors);
		double initValue = -1;
		
		if(maxExtensionValuesPerState[fstState][maxLength] == initValue) {
			double maxAdditionalInformationGain = getMaxInformationGain(prefix, maxLength, fstState, maxRepetitionList, globalDataCollectors);
			maxInformationGain = maxInformationGain + maxAdditionalInformationGain;
			maxExtensionValuesPerState[fstState][maxLength] = maxAdditionalInformationGain;
		} else {
//			System.out.println("buffer");
			maxInformationGain = maxInformationGain + maxExtensionValuesPerState[fstState][maxLength];
		}
		
		return maxInformationGain;
	}
	
	
	private double getMaxInformationGain(int[] prefix, int maxLength, int fstState, Int2IntOpenHashMap maxRepetitionList, Int2IntOpenHashMap localEdgeMaxCycles, HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors) {
		
//		maxLength = Integer.min(maxLength,fstGraph.getMaxTransitionsToFinalState(fstState));
		
//		ArrayList<Integer> fstStateItems = stateItems.get(fstState);
		
		
		ArrayList<EdgeItem> fstEdgeItems = new ArrayList<EdgeItem>();
		ArrayList<Integer> repetitionItems = new ArrayList<Integer>();
		repetitionItems.addAll(maxRepetitionList.keySet());
		fstEdgeItems = getFstEdgeItems(fstState, repetitionItems);
//		fstStateItems.addAll();
//		fstStateItems.sort(new Comparator<Integer>() {
//			@Override
//			public int compare(Integer o1, Integer o2) {
//				return Double.compare(gainList[o1], gainList[o2]) * -1;
//			}
//		});
		
//		HashSet<Integer> outputItems = generateOutputItems(xFst, fstState, maxRepetitionList);
		double maxInformationGain = 0;
		
		for(int i = 0; i<fstEdgeItems.size(); i++) {
			Integer maxRepetition = maxRepetitionList.get(fstEdgeItems.get(i).itemId);
			Integer maxEdgeRepetitions = localEdgeMaxCycles.get(fstEdgeItems.get(i).tId);
			if(maxRepetition != null) {
				while(maxLength > 0  && maxRepetition > 0 && maxEdgeRepetitions > 0) {
					maxInformationGain = maxInformationGain + gainList[fstEdgeItems.get(i).itemId];
					maxLength--;
					maxRepetition--;
					maxEdgeRepetitions--;
					localEdgeMaxCycles.addTo(fstEdgeItems.get(i).tId, -1);
					maxRepetitionList.addTo(fstEdgeItems.get(i).itemId, -1);
				}
				
				if(maxLength <= 0) {
					break;
				}
			}
		}
//		System.out.println(maxInformationGain);
		return maxInformationGain;
	}
	
//	private double getMaxInformationGain(int[] prefix, int maxLength, int fstState, Int2IntOpenHashMap maxRepetitionList, HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors) {
//		double maxInformationGain = getScoreBySequence(prefix, globalDataCollectors);
////		maxLength = Integer.min(maxLength,fstGraph.getMaxTransitionsToFinalState(fstState));
//		
////		ArrayList<Integer> fstStateItems = stateItems.get(fstState);
//		
//		
//		ArrayList<Integer> fstStateItems = new ArrayList<Integer>();
//		ArrayList<Integer> repetitionItems = new ArrayList<Integer>();
//		repetitionItems.addAll(maxRepetitionList.keySet());
//		fstStateItems = getStateItems(fstState, repetitionItems);
////		fstStateItems.addAll();
////		fstStateItems.sort(new Comparator<Integer>() {
////			@Override
////			public int compare(Integer o1, Integer o2) {
////				return Double.compare(gainList[o1], gainList[o2]) * -1;
////			}
////		});
//		
////		HashSet<Integer> outputItems = generateOutputItems(xFst, fstState, maxRepetitionList);
//		
//		for(int i = 0; i<fstStateItems.size(); i++) {
//			Integer maxRepetition = maxRepetitionList.get(fstStateItems.get(i));
//			if(maxRepetition != null) {
//				while(maxLength > 0  && maxRepetition > 0) {
//					maxInformationGain = maxInformationGain + gainList[fstStateItems.get(i)];
//					maxLength--;
//					maxRepetition--;
//				}
//				
//				if(maxLength <= 0) {
//					break;
//				}
//			}
//		}
////		System.out.println(maxInformationGain);
//		return maxInformationGain;
//	}
//	
	private double getMaxInformationGain(int[] prefix, int maxLength, int fstState, int[] maxRepetitionList, HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors) {
//		double maxInformationGain = getScoreBySequence(prefix, globalDataCollectors);
		
		Integer[] fstStateItems = (Integer[]) stateItems[fstState];
		
		double maxInformationGain = 0;
		
		for(int i = 0; i<fstStateItems.length; i++) {
			int itemId = fstStateItems[i];
			int maxRepetition = maxRepetitionList[itemId];
			// TODO: here I could optimize the array access rates
			if(maxRepetition > 0 && maxLength > 0) {
				if(maxLength > maxRepetition) {
					maxInformationGain = maxInformationGain + ( gainList[itemId] * maxRepetition );
					maxLength = maxLength - maxRepetition;
				} else {
					maxInformationGain = maxInformationGain + ( gainList[itemId] * maxLength );
					maxLength = 0;
				}
			}
			
			if(maxLength <= 0) {
				break;
			}
		}
		
		return maxInformationGain;
	}
	
	private double getMaxInformationGain(int[] prefix, int maxLength, int fstState, SparseBitSet itemOccurrence) {
		
//		Integer[] fstStateItems = (Integer[]) stateItems[fstState];
		
		double maxInformationGain = 0;
		// invert item id in order to get the correct id for information gain
		int itemId = itemOccurrence.nextSetBit(0);
		itemId = itemId * -1 + itemOccurrence.length();
		if(maxLength > 0 && itemId != 0) {
			return maxInformationGain = gainList[itemId] * maxLength;
		}
		
		return maxInformationGain;
	}
	
	private HashSet<Integer> generateOutputItems(XFst xFst, int state, ArrayList<Integer> items) {
		HashSet<Integer> outputItems = new HashSet<Integer>();
		for (Iterator<Integer> iterator = items.iterator(); iterator.hasNext();) {
			Integer itemId = (Integer) iterator.next();
			if (xfst.hasOutgoingTransition(state, itemId)) {
				for (int tId = 0; tId < xfst.numTransitions(state); ++tId) {
					if (xfst.canStep(itemId, state, tId)) {

						OutputLabel olabel = xfst.getOutputLabel(state, tId);
	
						switch (olabel.type) {
						case EPSILON:
							
							break;
	
						case CONSTANT:
							outputItems.add(olabel.item);
							break;
	
						case SELF:
							outputItems.add(itemId);
							break;
	
						case SELFGENERALIZE:
							outputItems.addAll(getParents(itemId, olabel.item));
							break;
	
						default:
							break;
						}
					}
				}
			}
		}
		
		return outputItems;
	}
	
	
	private HashSet<Integer> generateEdgeOutput(XFst xFst, int fromState, int transitionId, ArrayList<Integer> items) {
		HashSet<Integer> outputItems = new HashSet<Integer>();
		for (Iterator<Integer> iterator = items.iterator(); iterator.hasNext();) {
			int itemId = iterator.next();
			if (xfst.canStep(itemId, fromState, transitionId)) {

				OutputLabel olabel = xfst.getOutputLabel(fromState, transitionId);

				switch (olabel.type) {
				case EPSILON:
					
					break;

				case CONSTANT:
					outputItems.add(olabel.item);
					break;

				case SELF:
					outputItems.add(itemId);
					break;

				case SELFGENERALIZE:
					outputItems.addAll(getParents(itemId, olabel.item));
					break;

				default:
					break;
				}
			}
		}
		
		return outputItems;
	}
	
	private HashSet<Integer> generateOutputItems(XFst xFst, int state, int[] flist) {
		
		HashSet<Integer> outputItems = new HashSet<Integer>();
		for (int itemId = 0; itemId < flist.length; itemId++) {
			if (xfst.hasOutgoingTransition(state, itemId)) {
				for (int tId = 0; tId < xfst.numTransitions(state); ++tId) {
					if (xfst.canStep(itemId, state, tId)) {

						OutputLabel olabel = xfst.getOutputLabel(state, tId);
	
						switch (olabel.type) {
						case EPSILON:
							
							break;
	
						case CONSTANT:
							outputItems.add(olabel.item);
							break;
	
						case SELF:
							outputItems.add(itemId);
							break;
	
						case SELFGENERALIZE:
							outputItems.addAll(getParents(itemId, olabel.item));
							break;
	
						default:
							break;
						}
					}
				}
			}
		}
		
		return outputItems;
	}
		
	private ArrayList<Integer> getParents(int itemId, int rootItemId) {
		ArrayList<Integer> stack = new ArrayList<Integer>();
		IntOpenHashSet tempAnc = new IntOpenHashSet();
		
		int top = 0;
		stack.add(itemId);
		tempAnc.add(itemId);
		while (top < stack.size()) {
			int currItemId = stack.get(top);
			for (int parentId : Dictionary.getInstance().getParents(currItemId)) {
				if (xfst.isReachable(rootItemId, parentId) && !tempAnc.contains(parentId)) {
					stack.add(parentId);
					tempAnc.add(parentId);
				}
			}
			top++;
		}
		tempAnc.clear();
		return stack;
	}
	
	private void createInformationGainList(HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors, int[] flist) {
		this.gainList = new double[Dictionary.getInstance().getFlist().length];
				
		@SuppressWarnings("unchecked")
		Function<GlobalEventsCountCollector, Integer> eventsCountFunction = (Function<GlobalEventsCountCollector, Integer>) globalDataCollectors.get("TOTAL_EVENT_COUNT").finisher();
		int eventsCount = eventsCountFunction.apply((GlobalEventsCountCollector) globalDataCollectors.get("TOTAL_EVENT_COUNT"));
		
		
		ItemSupportCollector sup = (ItemSupportCollector) globalDataCollectors.get(ItemSupportCollector.ID);
		@SuppressWarnings("unchecked")
		Function<ItemSupportCollector, int[]> func = (Function<ItemSupportCollector, int[]>) globalDataCollectors.get(ItemSupportCollector.ID).finisher();
		
		for (int itemId = 0; itemId < flist.length; itemId++) {
			gainList[itemId] = (-1 * (Math.log(((double)func.apply(sup)[itemId]) / ((double) eventsCount)) / Math.log(func.apply(sup).length)));
		}
	}
	
	private void createStateItems(HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors) {
		boolean[] visitedFstStates = new boolean[xFst.numStates()]; 
		fstStateList = new HashMap<Integer, SortedSet<Double>>();	
		HashSet<Integer> outputItems = new HashSet<Integer>();
		
		ItemSupportCollector sup = (ItemSupportCollector) globalDataCollectors.get(ItemSupportCollector.ID);
		@SuppressWarnings("unchecked")
		Function<ItemSupportCollector, int[]> func = (Function<ItemSupportCollector, int[]>) globalDataCollectors.get(ItemSupportCollector.ID).finisher();
		int[] flist = func.apply(sup);
		
		if(gainList == null) {
			createInformationGainList(globalDataCollectors,Dictionary.getInstance().getFlist());
		}
		
		for(int i = 0; i<xFst.numStates();i++) {
			List<FstEdge> edge = fstGraph.getReachableEdgesPerState(i);
			ArrayList<Integer> itemList = new ArrayList<Integer>();
			for (Iterator<FstEdge> iterator = edge.iterator(); iterator.hasNext();) {
				FstEdge fstEdge = (FstEdge) iterator.next();
				
				if(visitedFstStates[fstEdge.getFromState()] != true) {
					outputItems.addAll(generateOutputItems(xFst, fstEdge.getFromState(),flist));
				}
	
				visitedFstStates[fstEdge.getFromState()] = true;
			}
			Arrays.fill(visitedFstStates, false);
			for (Iterator<Integer> iterator = outputItems.iterator(); iterator.hasNext();) {
				Integer itemId = (Integer) iterator.next();
				itemList.add(itemId);
			}
			
			itemList.sort(new Comparator<Integer>() {
					@Override
					public int compare(Integer o1, Integer o2) {
						return Double.compare(gainList[o1], gainList[o2]) * -1;
					}
				});
			
			stateItems[i] = itemList.toArray(new Integer[itemList.size()]);
			outputItems.clear();
		}
	}
	
	private ArrayList<Integer> getStateItems(int state, ArrayList<Integer> items) {
		boolean[] visitedFstStates = new boolean[xFst.numStates()]; 
		HashSet<Integer> outputItems = new HashSet<Integer>();
		
		List<FstEdge> edge = fstGraph.getReachableEdgesPerState(state);
		ArrayList<Integer> itemList = new ArrayList<Integer>();
		
		for (Iterator<FstEdge> iterator = edge.iterator(); iterator.hasNext();) {
			FstEdge fstEdge = (FstEdge) iterator.next();
			
			if(visitedFstStates[fstEdge.getFromState()] != true) {
				outputItems.addAll(generateOutputItems(xFst, fstEdge.getFromState(),items));
			}

			visitedFstStates[fstEdge.getFromState()] = true;
		}
		
		for (Iterator<Integer> iterator = outputItems.iterator(); iterator.hasNext();) {
			Integer itemId = (Integer) iterator.next();
			itemList.add(itemId);
		}
		
		itemList.sort(new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					return Double.compare(gainList[o1], gainList[o2]) * -1;
				}
			});
		
		outputItems.clear();
		return itemList;
	}
	
	private ArrayList<EdgeItem> getFstEdgeItems(int state, ArrayList<Integer> items) {
		HashMap<Integer, HashSet<Integer>> outputItems = new HashMap<Integer, HashSet<Integer>>();
		List<FstEdge> edge = fstGraph.getReachableEdgesPerState(state);
		ArrayList<EdgeItem> itemList = new ArrayList<EdgeItem>();
		
		for (Iterator<FstEdge> iterator = edge.iterator(); iterator.hasNext();) {
			FstEdge fstEdge = (FstEdge) iterator.next();
			HashSet<Integer> edgeItems = generateEdgeOutput(xFst, fstEdge.getFromState(), fstEdge.getTransitionId(),items);
			
			for (Iterator itemIterator = edgeItems.iterator(); itemIterator.hasNext();) {
				Integer itemId = (Integer) itemIterator.next();
				itemList.add(new EdgeItem(fstEdge.getTransitionId(), itemId));
			}
		}
		
		itemList.sort(new Comparator<EdgeItem>() {
				@Override
				public int compare(EdgeItem o1, EdgeItem o2) {
					return Double.compare(gainList[o1.itemId], gainList[o2.itemId]) * -1;
				}
			});
		
		outputItems.clear();
		return itemList;
	}
	
	private class EdgeItem {
		int tId;
		int itemId;
		
		public EdgeItem(int tId, int itemId) {
			this.tId = tId;
			this.itemId = itemId;
		}
	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public double getMaxScoreByPrefix(
//			int[] prefix,
//			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollector) {
//		double maxScore = 0;
//
//		int prefixSupport = ((Function<PrefixSupportCollector, Integer>) statData.get("PREFIXSUPPORT").finisher()).apply((PrefixSupportCollector) statData.get("PREFIXSUPPORT")) ;
//		int maxTransactionLength = ((Function<MaxRemainingTransactionLengthCollector, Integer>) statData.get("MAX_REMAIN_TRANSACTION_LENGTH").finisher()).apply((MaxRemainingTransactionLengthCollector) statData.get("MAX_REMAIN_TRANSACTION_LENGTH")) ;
//		@SuppressWarnings("unchecked")
//		HashSet<Integer> fstStates = (HashSet<Integer>) statData.get("FST_STATES");
//		buildItemMap((Int2IntOpenHashMap) statData.get("LOCAL_ITEM_FREQUENCIES"));
//		
//		// TODO: one could determine whether the reachable Edges of one state is a subset of the another state => only the superset needs to processed (worst case assumption)
//		// TODO: improvement by determining the reachable states for each accepted state ... disallow union of possible transitions (tighter bound)
//		// TODO: get the maximum number of cycle iterations => challenge: how to treat the "." -> many possible configurations possible
//		boolean firstIteration = true;
//		
//		for (int  fstState : fstStates) {
//			int stateMaxTransactionLength = maxTransactionLength;
//			ArrayList<Integer> stateMaxSequence = new ArrayList<Integer>();
//			
//			if(!firstIteration) {
//				// reset the state counts
//				itemMap.entrySet().forEach(entry -> {
//					entry.getValue().resetStateCount = true;
//				});
//				firstIteration = false;
//			}
//			
//			ArrayList<Integer> stateItems = stateValidItems.get(fstState);
//
//			// for each item in the priority queue until max transaction length or priorityQueue is empty
//			for(Integer maxScoreItemId : stateItems) {
//				ScoredHierarchyItem maxScoreItem = itemMap.get(maxScoreItemId);
//				if(maxScoreItem == null) {
//					continue;
//				}
//				
//				// reset the occurrence count of this item
//				int maxItemOccurrences = 0;
//				int itemsAddedCount = 0;
//
//				maxItemOccurrences = getMaxItemOccurrence(maxScoreItemId, maxScoreItem.getStateCount());
//				int numValidEdges = stateItemCycleIndicator.get(maxScoreItemId);
//				
//				
//				if(numValidEdges == -1) {
//					// yes
//					// is valid item part of cycle?	
//					// yes -> for loop until no more items or max transaction length
//
//					while(maxItemOccurrences > 0 && stateMaxTransactionLength > 0) {
//						// reduce the counters
//						maxItemOccurrences--;
//						stateMaxTransactionLength--;
//						itemsAddedCount++;
//						
//						// add the item to the sequence
//						stateMaxSequence.add(maxScoreItem.getItemId());
//					}
//					
//				} else {	
//					// no -> use max item
//					while(maxItemOccurrences > 0 && stateMaxTransactionLength > 0 && numValidEdges > 0) {
//						stateMaxTransactionLength--;
//						maxItemOccurrences--;
//						numValidEdges--;
//						itemsAddedCount++;
//						
//						// get max item and add it to the max sequence if count of parents is not 0
//						stateMaxSequence.add(maxScoreItem.getItemId());
//					}
//				}
//				if(stateMaxTransactionLength == 0) {
//					// stop processing, item cannot be added anymore
//					break;
//				} else {
//					updateStateCountIndex(maxScoreItem, itemsAddedCount);
//				}	
//			}
//			
////			System.out.println("Generated Array: " + Arrays.toString(stateMaxSequence.toArray(new String[stateMaxSequence.size()])));
//			
//			double sequenceScore = getScore(concatenateSequence(items, stateMaxSequence), statData, prefixSupport);
//			
//			if(sequenceScore > maxScore) {
//				maxScore = sequenceScore;
//			}
//		}
////		System.out.println(Arrays.toString(items) + maxScore);
//		return maxScore;
//	}
	
//	@Override
//	public boolean isSequenceExpandable(int[] prefix, SPMLocalStatisticCollector statisticCollector) {
//		double maxScore = 0;
//		
//		TransactionStateItemStatistic localStatistic = ((TransactionStateItemStatistic) statisticCollector);
//		buildItemMap(localStatistic);
//
//		// TODO: one could determine whether the reachable Edges of one state is a subset of the another state => only the superset needs to processed (worst case assumption)
//		// TODO: improvement by determining the reachable states for each accepted state ... disallow union of possible transitions (tighter bound)
//		// TODO: get the maximum number of cycle iterations => challenge: how to treat the "." -> many possible configurations possible
//		boolean firstIteration = true;
//		
//		for (int  pFSTState : localStatistic.getpFSTStates()) {
//			int stateMaxTransactionLength = localStatistic.getMaxTransactionLength();
//			
//			List<FstEdge> reachableEdges = fstGraph.getReachableEdgesPerState(pFSTState);
//			ArrayList<Integer> stateMaxSequence = new ArrayList<Integer>();
//			
//			if(!firstIteration) {
//				// reset the state counts
//				itemMap.entrySet().forEach(entry -> {
//					entry.getValue().resetStateCount = true;
//				});
//				firstIteration = false;
//			}
//
//			// for each item in the priority queue until max transaction length or priorityQueue is empty
//			for (Iterator<ScoredHierarchyItem> iterator = sortedItemSet.iterator(); iterator.hasNext();) {
//				ScoredHierarchyItem maxScoreItem = iterator.next();
//				
//				// reset the occurrence count of this item
//				int maxItemOccurrences = 0;
//				int itemsAddedCount = 0;
//
//				for (FstEdge edge : reachableEdges) {
//					if(edge.getLabel().equals(OutputLabel.Type.EPSILON)) {
//						// edge cannot create any output
//						continue;
//					}
//					
//					if(!edge.isWildcardTransition() && !checkIsOutputValid(edge, maxScoreItem.getItemId())) {
//						// no
//						// -> continue with next edge
//						continue;
//					} else {
//						// yes
//						// is edge part of cycle?
//						if(maxItemOccurrences == 0) {
//							maxItemOccurrences = getMaxItemOccurrence(maxScoreItem.getItemId(), maxScoreItem.getStateCount());;
//						}
//					
//						if(edge.isPartOfCylce()) {
//							// yes -> for loop until no more items or max transaction length
//							
//							
//							while(maxItemOccurrences > 0 && stateMaxTransactionLength > 0) {
//								// reduce the counters
//								maxItemOccurrences--;
//								stateMaxTransactionLength--;
//								itemsAddedCount++;
//								
//								// add the item to the sequence
//								stateMaxSequence.add(maxScoreItem.getItemId());
//							}
//						} else {	
//							// no -> use max item
//							if(maxItemOccurrences > 0 && stateMaxTransactionLength > 0) {
//								stateMaxTransactionLength--;
//								maxItemOccurrences--;
//								itemsAddedCount++;
//								
//								// get max item and add it to the max sequence if count of parents is not 0
//								stateMaxSequence.add(maxScoreItem.getItemId());
//							}
//						}
//						
//						if(maxItemOccurrences == 0 || stateMaxTransactionLength == 0) {
//							// stop processing, item cannot be added anymore
//							break;
//						}
//					}
//					
//					if(maxItemOccurrences == 0 || stateMaxTransactionLength == 0) {
//						// stop processing, item cannot be added anymore
//						break;
//					}
//				}
//				// maximum transaction length for the state reached
//				if(stateMaxTransactionLength == 0) {
//					break;
//				} else {
//					updateStateCountIndex(maxScoreItem, itemsAddedCount);
//				}
//			}
//			
////			System.out.println("Generated Array: " + Arrays.toString(stateMaxSequence.toArray(new String[stateMaxSequence.size()])));
//			
//			double sequenceScore = getSequenceScore(concatenateSequence(prefix, stateMaxSequence), localStatistic.getFrequency());
//			
//			if(sequenceScore > maxScore) {
//				maxScore = sequenceScore;
//			}
//		}
//
//		return isSequenceScoreSufficient(maxScore);
//	}

//	public void buildValidItemIndex() {
//		int numItems = Dictionary.getInstance().getFlist().length;
//		
//		for (int state : fstGraph.getStates()) {			
//			ArrayList<Integer> stateItems = new ArrayList<Integer>();	
//			List<FstEdge> reachableEdges = fstGraph.getReachableEdgesPerState(state);
//			// for each item in the priority queue until max transaction length or priorityQueue is empty			
//			for (int item = 0; item < numItems; item++) {
//				boolean addedItem = false;
//				for (FstEdge edge : reachableEdges) {
//					if(edge.getLabel().equals(OutputLabel.Type.EPSILON)) {
//						// edge cannot create any output
//						continue;
//					}
//					
//					
//					if(!edge.isWildcardTransition() && !checkItemMatchesOutput(edge, item)) {
//						// no
//						// -> continue with next edge
//						continue;
//					} else {
//						if(!addedItem) {
//							stateItems.add(item);
//						}
//						if(edge.isPartOfCylce() == true) {
//							stateItemCycleIndicator.put(item, stateItemCycleIndicator.get(item) + 1);
//							break;
//						} else {
//							stateItemCycleIndicator.put(item, stateItemCycleIndicator.get(item) + 1);
//						}
//					}
//				}
//			}
//			
//			stateItems.sort(new Comparator<Integer>() {
//				@Override
//				public int compare(Integer o1, Integer o2) {
//					return Double.compare(getItemScore(o1), getItemScore(o2)) * -1;
//				}
//			});
//			
//			stateValidItems.put(state, stateItems);
//		}
//		System.out.println("Item validity Index finished");
//	}
//	
//	private void buildItemMap(Int2IntOpenHashMap localItemFrequencies) {
//		ScoredHierarchyItem scoredItem;
//		
//		// creating lookup structure
//		// item id : count / score
//		for (Iterator<Entry> iterator = localItemFrequencies.int2IntEntrySet().fastIterator(); iterator.hasNext();) {
//			Map.Entry<Integer, Integer> itemFrequency = iterator.next();
//			int currentItem = itemFrequency.getKey();
//			
//			// add item itself
//			if(itemMap.containsKey(currentItem)) {
//				itemMap.get(currentItem).addCount(itemFrequency.getValue());
//			} else {
//				scoredItem = new ScoredHierarchyItem(currentItem, itemFrequency.getValue(), getItemScore(currentItem));
//				itemMap.put(currentItem, scoredItem);
////				sortedItemSet.add(scoredItem);
//			}
//			
//			// add all parents and sum counts
//			int parents[];
//			if(hierarchy.hasParent(currentItem)) {
//				parents = hierarchy.getAncestors(currentItem);
//				for(int i = 0; i<parents.length; i++) {
//					if(itemMap.containsKey(parents[i])) {
//						itemMap.get(parents[i]).addCount(itemFrequency.getValue());
//					} else {
//						scoredItem = new ScoredHierarchyItem(parents[i], itemFrequency.getValue(), getItemScore(currentItem));
//						itemMap.put(parents[i], scoredItem);
//	//					sortedItemSet.add(scoredItem);
//					}
//				}
//			}
//		}	
//	}
//	
//	private boolean checkItemMatchesOutput(FstEdge edge, int itemId) {
//
//		int s = edge.getFromState();
//		if (xFst.hasOutgoingTransition(s, itemId)) {
//			for (int tId = 0; tId < xFst.numTransitions(s); ++tId) {
//				if (xFst.canStep(itemId, s, tId)) {
//					int toState = xFst.getToState(s, tId);
//					OutputLabel olabel = xFst.getOutputLabel(s, tId);
//					
//					if(!edge.equals(new FstEdge(edge.getFromState(), toState, olabel))) {
//						// not the correct edge... search for next edge
//						continue;
//					}
//			
//					switch (olabel.type) {
//						case EPSILON:
//							// does not create output -> cannot be equal
//							return false;
//						case CONSTANT:
//							// needs to check whether the constant is equal to the input
//							return itemId == olabel.item;
//						case SELF:
//							// always true since the input item generates itself
//							return true;
//						case SELFGENERALIZE:
//							// always true since the input item generates itself
//							return true;
//						default:
//							break;
//					}
//				}
//			}
//		}
//
//		return false;
//	}
//	
//	private int getMaxItemOccurrence(int itemId, int itemCount) {
//		int minCount = itemCount;
//		
//		int currentItem = itemId;
//		int parents[];
//		if(hierarchy.hasParent(currentItem)) {
//			parents = hierarchy.getAncestors(currentItem);
//			for(int i = 0; i<parents.length; i++) {
//				if(itemMap.get(parents[i]).getStateCount() < minCount) {
//					minCount = itemMap.get(parents[i]).getStateCount();
//				}
//			}
//		}
//		
//		return minCount;
//	}
//	
//	private boolean checkIsOutputValid(FstEdge edge, int itemId) { 
//		boolean isValidOutput;
//		
//		byte edgeCacheValue = edge.getOutputCacheEntry(itemId);
//		switch (edgeCacheValue) {
//		case FstEdge.VALID_OUTPUT:
//			return true;
//		case FstEdge.INVALID_OUTPUT:
//			return false;
//		case FstEdge.CACHE_MISS:
//			isValidOutput = checkItemMatchesOutput(edge, itemId);
//			edge.addToOutputCache(itemId,isValidOutput);
//			return isValidOutput;
//		default:
//			return false;
//		}
//	}
//	
//	private void updateStateCountIndex(ScoredHierarchyItem item, int deductionCount) {
//		if(deductionCount != 0) {
//			item.decreaseStateCount(deductionCount);
//			
//			int currentItem = item.getItemId();
//			int parents[];
//			if(hierarchy.hasParent(currentItem)) {
//				parents = hierarchy.getAncestors(currentItem);
//				for(int i = 0; i<parents.length; i++) {
//					itemMap.get(parents[i]).decreaseStateCount(deductionCount);
//				}
//			}
//		}
//	}
//	
//	private int[] concatenateSequence(int[] prefix, ArrayList<Integer> suffix){
//		int[] items = new int[suffix.size() + prefix.length];
//		System.arraycopy(prefix, 0, items, 0, prefix.length);
//		
//		for (int i = 0; i < items.length - prefix.length; i++) {
//			items[i + prefix.length] = suffix.get(i);
//		}
//		
//		return items;
//	}
//	
//	private class ScoredHierarchyItem implements Comparable<ScoredHierarchyItem> {
//		private int count;
//		private double score;
//		private int itemId;
//		
//		private int stateCount;
//		
//		private boolean resetStateCount;
//		
//		public ScoredHierarchyItem(int itemId, int count, double score) {
//			this.itemId = itemId;
//			this.count = count;
//			this.score = score;
//			this.resetStateCount = true;
//		}
//		
//		public void addCount(int count) {
//			this.count = this.count + count;
//		}
//		
//		public void decreaseStateCount(int deduction) {
//			this.stateCount = this.stateCount - deduction;
//		}
//		
//		public int getStateCount() {
//			if(resetStateCount) {
//				stateCount = count;
//			}
//			return stateCount;
//		}
//		
//		public int getItemId() {
//			return itemId;
//		}	
//
//		@Override
//		public int compareTo(ScoredHierarchyItem o) {
//			return Double.compare(this.score, o.score) * -1;
//		}		
//	}
}
