package mining.statistics.old;


public class InformationGainScore {
//public class InformationGainScore extends DesqBaseScore implements DesqDfsScore {
//	FstGraph fstGraph;
//	XFst xFst;
//
//	RankedScoreList rankedScoreList;
//	Int2ObjectOpenHashMap<ScoredHierarchyItem> itemMap = new Int2ObjectOpenHashMap<ScoredHierarchyItem>();
//	Int2ObjectOpenHashMap<ArrayList<Integer>> stateValidItems = new Int2ObjectOpenHashMap<ArrayList<Integer>>();
//	Int2IntOpenHashMap stateItemCycleIndicator = new Int2IntOpenHashMap();
//	
//	public InformationGainScore(FstGraph fstGraph, XFst xFst) {
//		this.fstGraph = fstGraph;
//		this.xFst = xFst;
//		buildValidItemIndex();
//	}
//
//	@Override
//	public HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> getLocalCollectors() {
//		HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> collectors = new HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>();
//		collectors.put("PREFIXSUPPORT", (DesqProjDbDataCollector<?,?>) new PrefixSupportCollector());
////		collectors.put("PREFIXSUPPORT", new EventsCountCollector());
//		collectors.put("FST_STATES", (DesqProjDbDataCollector<?,?>) new FstStateItemCollector());
//		collectors.put("LOCAL_ITEM_FREQUENCIES", (DesqProjDbDataCollector<?,?>) new LocalItemFrequencyCollector());
//		collectors.put("MAX_REMAIN_TRANSACTION_LENGTH", (DesqProjDbDataCollector<?,?>) new MaxRemainingTransactionLengthCollector());
//		return collectors;
//	}
//
//	@SuppressWarnings("unchecked")
//	@Override
//	public double getMaximumScore(int[] items, HashMap<String,? extends DesqProjDbDataCollector<?,?>> statData) {
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
//	
////	@Override
////	public boolean isSequenceExpandable(int[] prefix, SPMLocalStatisticCollector statisticCollector) {
////		double maxScore = 0;
////		
////		TransactionStateItemStatistic localStatistic = ((TransactionStateItemStatistic) statisticCollector);
////		buildItemMap(localStatistic);
////
////		// TODO: one could determine whether the reachable Edges of one state is a subset of the another state => only the superset needs to processed (worst case assumption)
////		// TODO: improvement by determining the reachable states for each accepted state ... disallow union of possible transitions (tighter bound)
////		// TODO: get the maximum number of cycle iterations => challenge: how to treat the "." -> many possible configurations possible
////		boolean firstIteration = true;
////		
////		for (int  pFSTState : localStatistic.getpFSTStates()) {
////			int stateMaxTransactionLength = localStatistic.getMaxTransactionLength();
////			
////			List<FstEdge> reachableEdges = fstGraph.getReachableEdgesPerState(pFSTState);
////			ArrayList<Integer> stateMaxSequence = new ArrayList<Integer>();
////			
////			if(!firstIteration) {
////				// reset the state counts
////				itemMap.entrySet().forEach(entry -> {
////					entry.getValue().resetStateCount = true;
////				});
////				firstIteration = false;
////			}
////
////			// for each item in the priority queue until max transaction length or priorityQueue is empty
////			for (Iterator<ScoredHierarchyItem> iterator = sortedItemSet.iterator(); iterator.hasNext();) {
////				ScoredHierarchyItem maxScoreItem = iterator.next();
////				
////				// reset the occurrence count of this item
////				int maxItemOccurrences = 0;
////				int itemsAddedCount = 0;
////
////				for (FstEdge edge : reachableEdges) {
////					if(edge.getLabel().equals(OutputLabel.Type.EPSILON)) {
////						// edge cannot create any output
////						continue;
////					}
////					
////					if(!edge.isWildcardTransition() && !checkIsOutputValid(edge, maxScoreItem.getItemId())) {
////						// no
////						// -> continue with next edge
////						continue;
////					} else {
////						// yes
////						// is edge part of cycle?
////						if(maxItemOccurrences == 0) {
////							maxItemOccurrences = getMaxItemOccurrence(maxScoreItem.getItemId(), maxScoreItem.getStateCount());;
////						}
////					
////						if(edge.isPartOfCylce()) {
////							// yes -> for loop until no more items or max transaction length
////							
////							
////							while(maxItemOccurrences > 0 && stateMaxTransactionLength > 0) {
////								// reduce the counters
////								maxItemOccurrences--;
////								stateMaxTransactionLength--;
////								itemsAddedCount++;
////								
////								// add the item to the sequence
////								stateMaxSequence.add(maxScoreItem.getItemId());
////							}
////						} else {	
////							// no -> use max item
////							if(maxItemOccurrences > 0 && stateMaxTransactionLength > 0) {
////								stateMaxTransactionLength--;
////								maxItemOccurrences--;
////								itemsAddedCount++;
////								
////								// get max item and add it to the max sequence if count of parents is not 0
////								stateMaxSequence.add(maxScoreItem.getItemId());
////							}
////						}
////						
////						if(maxItemOccurrences == 0 || stateMaxTransactionLength == 0) {
////							// stop processing, item cannot be added anymore
////							break;
////						}
////					}
////					
////					if(maxItemOccurrences == 0 || stateMaxTransactionLength == 0) {
////						// stop processing, item cannot be added anymore
////						break;
////					}
////				}
////				// maximum transaction length for the state reached
////				if(stateMaxTransactionLength == 0) {
////					break;
////				} else {
////					updateStateCountIndex(maxScoreItem, itemsAddedCount);
////				}
////			}
////			
//////			System.out.println("Generated Array: " + Arrays.toString(stateMaxSequence.toArray(new String[stateMaxSequence.size()])));
////			
////			double sequenceScore = getSequenceScore(concatenateSequence(prefix, stateMaxSequence), localStatistic.getFrequency());
////			
////			if(sequenceScore > maxScore) {
////				maxScore = sequenceScore;
////			}
////		}
////
////		return isSequenceScoreSufficient(maxScore);
////	}
//
//	@Override
//	public double getScore(int[] prefix, HashMap<String,? extends DesqProjDbDataCollector<?,?>> statCollectors, int support) {
//		double totalInformationGain = 0;
//		for (int i = 0; i < prefix.length; i++) {
//			totalInformationGain = totalInformationGain + globalInformationGainStatistic.getInformationGain(prefix[i]);
//		}
//		return totalInformationGain;
////		return (totalInformationGain * (support - 1));
//	}
//
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
