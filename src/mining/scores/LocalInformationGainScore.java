package mining.scores;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.HashMap;
import java.util.function.Function;

import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.collectors.EventsCountCollector;
import mining.statistics.collectors.FstStateItemCollector;
import mining.statistics.collectors.MaxRemainingTransactionLengthCollector;
import mining.statistics.collectors.PrefixSupportCollector;
import mining.statistics.collectors.ProjDatabaseFrequencyCollector;
import fst.XFst;

public class LocalInformationGainScore extends DesqBaseScore implements DesqDfsScore {	
		

	public LocalInformationGainScore(XFst xfst) {
		super(xfst);
	}

	@Override
	public HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> getProjDbCollectors() {
		HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> collectors = new HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>();
		collectors.put("PREFIXSUPPORT", (DesqProjDbDataCollector<?, ?>) new PrefixSupportCollector());
		collectors.put("TOTAL_EVENT_COUNT", (DesqProjDbDataCollector<?,?>) new EventsCountCollector());
		collectors.put("FST_STATES", (DesqProjDbDataCollector<?,?>) new FstStateItemCollector());
		collectors.put("PROJ_DB_FREQUENCIES", (DesqProjDbDataCollector<?,?>) new ProjDatabaseFrequencyCollector());
		collectors.put("MAX_REMAIN_TRANSACTION_LENGTH", (DesqProjDbDataCollector<?,?>) new MaxRemainingTransactionLengthCollector());
		return collectors;
	}

	@Override
	public double getScoreByProjDb(int[] sequence, 
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>> finalStateProjDbCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>>[] prefixProjDbCollectors) {
		
		double totalInformationGain = 0;

		
//		PrefixSupportCollector sup = (PrefixSupportCollector) prefixProjDbCollectors[prefixProjDbCollectors.length - 1].get("PREFIXSUPPORT");
		
//		@SuppressWarnings("unchecked")
//		Function<PrefixSupportCollector, Integer> func = (Function<PrefixSupportCollector, Integer>) prefixProjDbCollectors[prefixProjDbCollectors.length - 1].get("PREFIXSUPPORT").finisher();
//		EventsCountCollector eventsCountCollector = (EventsCountCollector) prefixProjDbCollectors[prefixProjDbCollectors.length].get("TOTAL_EVENT_COUNT");
		@SuppressWarnings("unchecked")
		Function<EventsCountCollector, Integer> eventsCountFunction = (Function<EventsCountCollector, Integer>) prefixProjDbCollectors[prefixProjDbCollectors.length].get("TOTAL_EVENT_COUNT").finisher();
		
//		ProjDatabaseFrequencyCollector projDbFrequencyCollector = (ProjDatabaseFrequencyCollector) prefixProjDbCollectors[prefixProjDbCollectors.length].get("PROJ_DB_FREQUENCIES");
		@SuppressWarnings("unchecked")
		Function<ProjDatabaseFrequencyCollector, Int2IntOpenHashMap> projDbFrequencyFunction = (Function<ProjDatabaseFrequencyCollector, Int2IntOpenHashMap>) prefixProjDbCollectors[prefixProjDbCollectors.length].get("PROJ_DB_FREQUENCIES").finisher();

		for (int i = 0; i < sequence.length; i++) {
			int eventsCount = eventsCountFunction.apply((EventsCountCollector) prefixProjDbCollectors[i].get("TOTAL_EVENT_COUNT"));
			Int2IntOpenHashMap projDBItemFrequencies = (Int2IntOpenHashMap) projDbFrequencyFunction.apply((ProjDatabaseFrequencyCollector) prefixProjDbCollectors[i].get("PROJ_DB_FREQUENCIES"));			
			
			double itemInformationGain = 0;
			itemInformationGain = -1 * (Math.log((double) projDBItemFrequencies.get(sequence[i]) / ((double) eventsCount)) / Math.log(projDBItemFrequencies.size()));
			
			if(Double.isInfinite(itemInformationGain)) {
				System.out.println(projDBItemFrequencies.get(sequence[i]));
				System.out.println((double) eventsCount);
				System.out.println(Math.log(projDBItemFrequencies.size()));
				System.out.println("INFINITE");
			}
			totalInformationGain = totalInformationGain + itemInformationGain;
		}
		
		return totalInformationGain;
	}
}
