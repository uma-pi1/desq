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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;

import com.zaxxer.sparsebits.SparseBitSet;

import fst.OutputLabel;
import fst.OutputLabel.Type;
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

public class InformationGainScoreDesqCountNoPruning extends DesqBaseScore {
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
	
	public InformationGainScoreDesqCountNoPruning(XFst xFst) {
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
}
