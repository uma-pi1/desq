package mining.scores;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.function.Function;

import fst.XFst;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.collectors.EventsCountCollector;
import mining.statistics.collectors.GlobalTransactionsCounter;
import mining.statistics.collectors.ItemSupportCollector;
import mining.statistics.collectors.PrefixSupportCollector;
import mining.statistics.collectors.ProjDatabaseFrequencyCollector;
import utils.Dictionary;

public class LocalInformationGainScore extends DesqBaseScore implements DesqDfsScore {	
		
	
	public static int MIN_SUPPORT = 100;
	
	public LocalInformationGainScore(XFst xfst) {
		super(xfst);
	}
	
	@Override
	public HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> getGlobalDataCollectors() {
		HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> collectors = new HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>>();
		collectors.put(ItemSupportCollector.ID, new ItemSupportCollector());
		collectors.put(GlobalTransactionsCounter.ID, new GlobalTransactionsCounter());
		
		return collectors;
	}
	

	@Override
	public HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> getProjDbCollectors() {
		HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> collectors = new HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>();
		collectors.put("PREFIXSUPPORT", (DesqProjDbDataCollector<?, ?>) new PrefixSupportCollector());
		collectors.put("TOTAL_EVENT_COUNT", (DesqProjDbDataCollector<?,?>) new EventsCountCollector());
		collectors.put("PROJ_DB_FREQUENCIES", (DesqProjDbDataCollector<?,?>) new ProjDatabaseFrequencyCollector());
		return collectors;
	}
	
	@Override
	public double getMaxScoreByItem(
			int item,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors) {
		
		ItemSupportCollector sup = (ItemSupportCollector) globalDataCollectors.get(ItemSupportCollector.ID);
		
		@SuppressWarnings("unchecked")
		Function<ItemSupportCollector, int[]> func = (Function<ItemSupportCollector, int[]>) globalDataCollectors.get(ItemSupportCollector.ID).finisher();		
		
		if(func.apply(sup)[item] >= MIN_SUPPORT) {
			return Double.MAX_VALUE;
		} else {
			return 0;
		}
	}
	
	public double getMaxScoreByPrefix(int[] prefix,  
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>>[] prefixProjDbCollectors) {
		
		PrefixSupportCollector sup = (PrefixSupportCollector) prefixProjDbCollectors[prefixProjDbCollectors.length - 1].get("PREFIXSUPPORT");
		@SuppressWarnings("unchecked")
		Function<PrefixSupportCollector, Integer> func = (Function<PrefixSupportCollector, Integer>) prefixProjDbCollectors[prefixProjDbCollectors.length - 1].get("PREFIXSUPPORT").finisher();
		
		Integer support = func.apply(sup);
		
		if(support >= MIN_SUPPORT) {
			return Double.MAX_VALUE;
		} else {
			return 0;
		}
		
	}

	@Override
	public double getScoreByProjDb(int[] sequence, 
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>> finalStateProjDbCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>>[] prefixProjDbCollectors) {
		
		double totalInformationGain = 0;

		
		PrefixSupportCollector sup = (PrefixSupportCollector) finalStateProjDbCollectors.get("PREFIXSUPPORT");
		@SuppressWarnings("unchecked")
		Function<PrefixSupportCollector, Integer> func = (Function<PrefixSupportCollector, Integer>) finalStateProjDbCollectors.get("PREFIXSUPPORT").finisher();
		
		Integer support = func.apply(sup);
		
		if(support >= MIN_SUPPORT) {
			@SuppressWarnings("unchecked")
			Function<EventsCountCollector, Integer> eventsCountFunction = (Function<EventsCountCollector, Integer>) prefixProjDbCollectors[prefixProjDbCollectors.length - 1].get("TOTAL_EVENT_COUNT").finisher();
			
			@SuppressWarnings("unchecked")
			Function<ProjDatabaseFrequencyCollector, Int2IntOpenHashMap> projDbFrequencyFunction = (Function<ProjDatabaseFrequencyCollector, Int2IntOpenHashMap>) prefixProjDbCollectors[prefixProjDbCollectors.length - 1].get("PROJ_DB_FREQUENCIES").finisher();
	
			for (int i = 0; i < sequence.length; i++) {
				int eventsCount = eventsCountFunction.apply((EventsCountCollector) prefixProjDbCollectors[i].get("TOTAL_EVENT_COUNT"));
				Int2IntOpenHashMap projDBItemFrequencies = (Int2IntOpenHashMap) projDbFrequencyFunction.apply((ProjDatabaseFrequencyCollector) prefixProjDbCollectors[i].get("PROJ_DB_FREQUENCIES"));			
				
				
				double itemFrequency;
				
				if(projDBItemFrequencies.containsKey(sequence[i])) {
					itemFrequency = (double) projDBItemFrequencies.get(sequence[i]);
				} else {				
					itemFrequency = (double) getParentFrequency(projDBItemFrequencies, sequence[i]);
					
				}
				
				double itemInformationGain = 0;
				itemInformationGain = -1 * (Math.log(itemFrequency / ((double) eventsCount)) / Math.log(projDBItemFrequencies.size()));
				
				if(Double.isInfinite(itemInformationGain)) {
					System.out.println(projDBItemFrequencies.get(sequence[i]));
					System.out.println(itemFrequency);
					System.out.println((double) eventsCount);
					System.out.println(Math.log(projDBItemFrequencies.size()));
					System.out.println("INFINITE");
				}
				totalInformationGain = totalInformationGain + itemInformationGain;
			}
			
			return totalInformationGain;
			
		} else {
			return 0;
		}
	}
	
	private int getParentFrequency(Int2IntOpenHashMap projDBItemFrequencies, int itemId) {
		int parentFrequency = 0;
		for (ObjectIterator<Entry<Integer, Integer>> iterator = projDBItemFrequencies.entrySet().iterator(); iterator.hasNext();) {
			Entry<Integer, Integer> projDbFrequency = (Entry<Integer, Integer>) iterator.next();
			
			int[] ancestors = Dictionary.getInstance().getAncestors(projDbFrequency.getKey());
			
			for (int i = 0; i < ancestors.length; i++) {
				if(ancestors[i] == itemId) {
					parentFrequency = parentFrequency + projDbFrequency.getValue();
				}
			}
		}
		return parentFrequency;
		
	}
}
