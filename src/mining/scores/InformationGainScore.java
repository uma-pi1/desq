package mining.scores;

import it.unimi.dsi.fastutil.ints.Int2DoubleAVLTreeMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.function.Function;

import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.collectors.FstStateItemCollector;
import mining.statistics.collectors.GlobalEventsCountCollector;
import mining.statistics.collectors.GlobalItemMaxRepetitionCollector;
import mining.statistics.collectors.GlobalMaxTransactionLengthCollector;
import mining.statistics.collectors.ItemSupportCollector;
import mining.statistics.collectors.LocalItemFrequencyCollector;
import mining.statistics.collectors.MaxRemainingTransactionLengthCollector;
import mining.statistics.collectors.PrefixSupportCollector;
import tools.FstEdge;
import tools.FstGraph;
import utils.Dictionary;
import fst.OutputLabel;
import fst.XFst;

public class InformationGainScore extends DesqBaseScore {
	FstGraph fstGraph;
	XFst xFst;
	HashMap<Integer, SortedSet<Double>> fstStateList;
	double[] gainList;
	ArrayList<ArrayList<Integer>> stateItems = new ArrayList<ArrayList<Integer>>();
	
//	RankedScoreList rankedScoreList;
//	Int2ObjectOpenHashMap<ScoredHierarchyItem> itemMap = new Int2ObjectOpenHashMap<ScoredHierarchyItem>();
//	Int2ObjectOpenHashMap<ArrayList<Integer>> stateValidItems = new Int2ObjectOpenHashMap<ArrayList<Integer>>();
//	Int2IntOpenHashMap stateItemCycleIndicator = new Int2IntOpenHashMap();
	
	public InformationGainScore(XFst xFst) {
		super(xFst);
		this.xFst = xFst;
		this.fstGraph = xFst.convertToFstGraph();
		this.gainList = new double[Dictionary.getInstance().getFlist().length];
//		buildValidItemIndex();
	}

	@Override
	public HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?,?>, ?>> getProjDbCollectors() {
		HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> collectors = new HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>();
		collectors.put("PREFIXSUPPORT", new PrefixSupportCollector());
		collectors.put("FST_STATES", new FstStateItemCollector());
		collectors.put("LOCAL_ITEM_FREQUENCIES", new LocalItemFrequencyCollector());
		collectors.put("MAX_REMAIN_TRANSACTION_LENGTH", new MaxRemainingTransactionLengthCollector());
		return collectors;
	}
	
	@Override
	public HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> getGlobalDataCollectors() {
		HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> collectors = new HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>>();
		collectors.put("TOTAL_EVENT_COUNT", new GlobalEventsCountCollector());
		collectors.put(ItemSupportCollector.ID, new ItemSupportCollector());
		collectors.put(GlobalItemMaxRepetitionCollector.ID, new GlobalItemMaxRepetitionCollector());
		collectors.put(GlobalMaxTransactionLengthCollector.ID, new GlobalMaxTransactionLengthCollector());
		
		return collectors;
	}
	

	@Override
	public double getScoreBySequence(int[] sequence, 
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors) {		
		
		if(gainList.length == 0) {
			createInformationGainList(globalDataCollectors,Dictionary.getInstance().getFlist());
		}
		if(sequence != null) {
			double totalInformationGain = 0;
			
			for (int i = 0; i < sequence.length; i++) {
				totalInformationGain = totalInformationGain + gainList[sequence[i]];
			}
			
			return totalInformationGain;
		} else {
			return 0;
		}
		
	}

	
	@Override
	public double getMaxScoreByPrefix(
			int[] prefix,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors,
			int[] transaction,
			int position,
			int fstState) {
		return Double.MAX_VALUE;
//		if(fstStateList == null) {
//			createStateItems(globalDataCollectors);
//		}
//		
//		GlobalItemMaxRepetitionCollector sup = (GlobalItemMaxRepetitionCollector) globalDataCollectors.get(GlobalItemMaxRepetitionCollector.ID);
//		@SuppressWarnings("unchecked")
//		Function<GlobalItemMaxRepetitionCollector, int[]> func = (Function<GlobalItemMaxRepetitionCollector, int[]>) globalDataCollectors.get(GlobalItemMaxRepetitionCollector.ID).finisher();
//		int[] maxRepetitionList = func.apply(sup);
//		
//		double maxInformationGain = getScoreBySequence(prefix, globalDataCollectors);
//		int maxLength = transaction.length - position;
//		
//		ArrayList<Integer> fstStateItems = stateItems.get(fstState);
//		
//		for(int i = 0; i<fstStateItems.size(); i++) {
//			int maxRepetition = maxRepetitionList[fstStateItems.get(i)]; 
//			while(maxLength != 0  && maxRepetition != 0) {
//				maxInformationGain = maxInformationGain + gainList[fstStateItems.get(i)];
//				maxLength--;
//				maxRepetition--;
//			}
//			
//			if(maxLength == 0) {
//				break;
//			}
//		}
//		
//		return maxInformationGain;
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
		
		if(gainList.length == 0) {
			createInformationGainList(globalDataCollectors,Dictionary.getInstance().getFlist());
		}
		
		for(int i = 0; i<xFst.numStates();i++) {
			List<FstEdge> edge = fstGraph.getReachableEdgesPerState(i);
			ArrayList<Integer> itemList = new ArrayList<Integer>();
			for (Iterator<FstEdge> iterator = edge.iterator(); iterator.hasNext();) {
				FstEdge fstEdge = (FstEdge) iterator.next();
				
				if(visitedFstStates[fstEdge.getFromState()] != true) {
					outputItems.addAll(generateOutputItems(xFst, fstEdge.getToState(),flist));
				}
	
				visitedFstStates[fstEdge.getFromState()] = true;
			}
			Arrays.fill(visitedFstStates, false);
			for (Iterator<Integer> iterator = outputItems.iterator(); iterator.hasNext();) {
				Integer itemId = (Integer) iterator.next();
				itemList.add(itemId);
			}
			stateItems.add(itemList);
			stateItems.get(i).sort(new Comparator<Integer>() {
					@Override
					public int compare(Integer o1, Integer o2) {
						return Double.compare(gainList[o1], gainList[o2]) * -1;
					}
				});
			outputItems.clear();
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
