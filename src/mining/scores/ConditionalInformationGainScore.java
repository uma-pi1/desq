package mining.scores;

import fst.OutputLabel;
import fst.XFst;
import mining.statistics.SPMLocalStatisticCollector;
import mining.statistics.TransactionStateItemStatistic;
import tools.FstGraph;
import utils.Hierarchy;

public class ConditionalInformationGainScore extends DesqBaseScore implements SPMScore {	
	double minInformationGain;
	FstGraph fstGraph;
	Hierarchy hierarchy;
	XFst xFst;
	int[] pFSTToStates;
	OutputLabel[] outputLabelArray;
	RankedScoreList rankedScoreList;
//	Int2ObjectOpenHashMap<ScoredHierarchyItem> itemMap = new Int2ObjectOpenHashMap<ScoredHierarchyItem>();
//	Int2ObjectOpenHashMap<ArrayList<Integer>> stateValidItems = new Int2ObjectOpenHashMap<ArrayList<Integer>>();
//	Int2IntOpenHashMap stateItemCycleIndicator = new Int2IntOpenHashMap();
	
//	Comparator<ScoredHierarchyItem> scoreComparator = new Comparator<ScoredHierarchyItem>() {
//        @Override public int compare(ScoredHierarchyItem o1, ScoredHierarchyItem o2) {
//            return o1.compareTo(o2);
//        }           
//    };
	
//    TreeSet<ScoredHierarchyItem> sortedItemSet = new TreeSet<ScoredHierarchyItem>(scoreComparator);
	
	public ConditionalInformationGainScore(FstGraph fstGraph, double minInformationGain, Hierarchy hierarchy, 
											XFst xFst, RankedScoreList rankedScoreList) {
		this.minInformationGain = minInformationGain;
		this.fstGraph = fstGraph;
		this.hierarchy = hierarchy;
		this.xFst = xFst;
		this.rankedScoreList = rankedScoreList;
		
		pFSTToStates = new int[xFst.toStates.size() + 1];
		xFst.toStates.toArray(pFSTToStates);
		
		outputLabelArray = new OutputLabel[xFst.outLabels.size()];
		xFst.outLabels.toArray(outputLabelArray);
	}

//	@Override
//	public boolean isSequenceScoreSufficient(double score) {
//		return (score >= minInformationGain && score >= rankedScoreList.getMinScore());
//	}

	@Override
	public double getScore(int[] prefix, SPMLocalStatisticCollector[] statisticCollector) {
		return 0.0;
	}

	@Override
	public double getMaximumScore(int[] items, int support, SPMLocalStatisticCollector[] sequenceStatistics) {
		double totalInformationGain = 0;
		if(support > 1) {
			for (int i = 0; i < items.length; i++) {
				double itemInformationGain = 0;
				if(i == 0) {
					TransactionStateItemStatistic itemStatistic = (TransactionStateItemStatistic) sequenceStatistics[i];
					itemInformationGain = -1 * (Math.log((double) itemStatistic.getLocalItemFrequencies().get(items[i]) / ((double) itemStatistic.getEventsCount())) / Math.log(itemStatistic.getLocalItemFrequencies().size()));
	//				System.out.println("InformationGain: " + itemInformationGain + " Frequency: " + itemStatistic.getLocalItemFrequencies().get(items[i]) + " Total events count: " + itemStatistic.getEventsCount() + " No of diff. Items: " + itemStatistic.getLocalItemFrequencies().size());
				} else {
					TransactionStateItemStatistic prevItemStatistic = (TransactionStateItemStatistic) sequenceStatistics[i-1];
					TransactionStateItemStatistic itemStatistic = (TransactionStateItemStatistic) sequenceStatistics[i];
					itemInformationGain = -1 * (Math.log((double) itemStatistic.getLocalItemFrequencies().get(items[i]) / ((double) itemStatistic.getEventsCount())) / Math.log(itemStatistic.getLocalItemFrequencies().size())) * (prevItemStatistic.getFrequency() - itemStatistic.getFrequency()) / prevItemStatistic.getFrequency();
	//				System.out.println("InformationGain: " + itemInformationGain + " Frequency: " + itemStatistic.getLocalItemFrequencies().get(items[i]) + " Total events count: " + itemStatistic.getEventsCount() + " No of diff. Items: " + itemStatistic.getLocalItemFrequencies().size());
				}
				
				totalInformationGain = totalInformationGain + itemInformationGain;
			}
		}
		return totalInformationGain;
	}
	
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
//	private void buildItemMap(TransactionStateItemStatistic localStatistic) {
//		ScoredHierarchyItem scoredItem;
//		
//		// creating lookup structure
//		// item id : count / score
//		for (Iterator<Entry> iterator = localStatistic.getLocalItemFrequencies().int2IntEntrySet().fastIterator(); iterator.hasNext();) {
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
//			while(hierarchy.hasParent(currentItem)) {
//				currentItem = hierarchy.getParent(currentItem);
//				
//				if(itemMap.containsKey(currentItem)) {
//					itemMap.get(currentItem).addCount(itemFrequency.getValue());
//				} else {
//					scoredItem = new ScoredHierarchyItem(currentItem, itemFrequency.getValue(), getItemScore(currentItem));
//					itemMap.put(currentItem, scoredItem);
////					sortedItemSet.add(scoredItem);
//				}
//			}
//		}	
//	}
//	
//	private boolean checkItemMatchesOutput(FstEdge edge, int itemId) {
//		int offset = pFST.getOffset(edge.getFromState(), itemId);
//		
//		if (offset >= 0) {
//			for (; pFSTToStates[offset] != 0; offset++) {
//				int toState = pFSTToStates[offset];
//				OutputLabel outLabel = outputLabelArray[offset];
//				
//				if(!edge.equals(new FstEdge(edge.getFromState(), toState, outLabel))) {
//					// not the correct edge... search for next edge
//					continue;
//				}
//				
//				int outputItem = outLabel.item;
//				
//				switch (outLabel.type) {
//					case EPSILON:
//						// does not create output -> cannot be equal
//						return false;
//					case CONSTANT:
//						// needs to check whether the constant is equal to the input
//						return itemId == outputItem;
//					case SELF:
//						// always true since the input item generates itself
//						return true;
//					case SELFGENERALIZE:
//						// always true since the input item generates itself
//						return true;
//					default:
//						break;
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
//		while(hierarchy.hasParent(currentItem)) {
//			currentItem = hierarchy.getParent(currentItem);
//			if(itemMap.get(currentItem).getStateCount() < minCount) {
//				minCount = itemMap.get(currentItem).getStateCount();
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
//			while(hierarchy.hasParent(currentItem)) {
//				currentItem = hierarchy.getParent(currentItem);
//				itemMap.get(currentItem).decreaseStateCount(deductionCount);
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
