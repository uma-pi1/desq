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
import mining.statistics.collectors.GlobalEdgeMaxCycleCollector;
import mining.statistics.collectors.GlobalEventsCountCollector;
import mining.statistics.collectors.GlobalItemMaxRepetitionCollector;
import mining.statistics.collectors.GlobalMaxTransactionLengthCollector;
import mining.statistics.collectors.ItemSupportCollector;
import tools.FstEdge;
import tools.FstGraph;
import utils.Dictionary;

public class InformationGainScoreDesqCount extends DesqBaseScore {
	FstGraph fstGraph;
	XFst xFst;
	HashMap<Integer, SortedSet<Double>> fstStateList;
	double[] gainList;
	Object[] stateItems;
	double[][] maxExtensionValuesPerState;
	FixedBitSet[] stateBitSets;
	SparseBitSet[] sparseStateBitSets; 
	Int2IntOpenHashMap itemOccurrenceCount = new Int2IntOpenHashMap();
	int previousTransactionId = -1;
	double maxUpToNow = 0;
	
	public InformationGainScoreDesqCount(XFst xFst) {
		super(xFst);
		this.xFst = xFst;
		this.fstGraph = xFst.convertToFstGraph();
		this.stateItems = new Object[xFst.numStates()];
	}
	
	@Override
	public HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?,?>, ?>> getProjDbCollectors() {
		HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> collectors = new HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>();

		return collectors;
	}
	
	@Override
	public HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> getGlobalDataCollectors() {
		HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> collectors = new HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>>();
		collectors.put("TOTAL_EVENT_COUNT", new GlobalEventsCountCollector());
		collectors.put(ItemSupportCollector.ID, new ItemSupportCollector());
		collectors.put(GlobalItemMaxRepetitionCollector.ID, new GlobalItemMaxRepetitionCollector());
		collectors.put(GlobalMaxTransactionLengthCollector.ID, new GlobalMaxTransactionLengthCollector());
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
	 * this function works based on global data for DESQ-COUNT
	 */
	@Override
	public double getMaxScoreByPrefix(
			int[] prefix,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors,
			int[] transaction,
			int transactionId,
			int position,
			int fstState,
			int[] transCount) {
		
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
		
		
		int maxLength = maxEdgeCycles.get(transactionId)[fstState];
		
		maxLength = Integer.min(maxLength, transaction.length - position);
		
		
		double maxInformationGain = getScoreBySequence(prefix, globalDataCollectors);
		double initValue = -1;
		if(maxLength >= 0) {
			if(maxExtensionValuesPerState[fstState][maxLength] == initValue) {
				double maxAdditionalInformationGain = getMaxInformationGain(prefix, maxLength, fstState, maxRepetitionList, globalDataCollectors);
				maxInformationGain = maxInformationGain + maxAdditionalInformationGain;
				maxExtensionValuesPerState[fstState][maxLength] = maxAdditionalInformationGain;
			} else {
				maxInformationGain = maxInformationGain + maxExtensionValuesPerState[fstState][maxLength];
			}
		}
		
		return maxInformationGain;
	}

	private double getMaxInformationGain(int[] prefix, int maxLength, int fstState, int[] maxRepetitionList, HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors) {	
		Integer[] fstStateItems = (Integer[]) stateItems[fstState];
		
		double maxInformationGain = 0;
		
		for(int i = 0; i<fstStateItems.length; i++) {
			int itemId = fstStateItems[i];
			int maxRepetition = maxRepetitionList[itemId];
			// TODO: here I could optimize the array access rates
			if(maxRepetition > 0 && maxLength > 0 && itemId != 0) {
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
}
