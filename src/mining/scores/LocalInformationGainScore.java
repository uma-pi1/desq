package mining.scores;

import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collector;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import mining.statistics.DesqDfsMiningCollectorBase;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.collectors.EventsCountCollector;
import mining.statistics.collectors.FstStateItemCollector;
import mining.statistics.collectors.MaxRemainingTransactionLengthCollector;
import mining.statistics.collectors.PrefixSupportCollector;
import mining.statistics.collectors.ProjDatabaseFrequencyCollector;

public class LocalInformationGainScore extends DesqBaseScore implements DesqDfsScore {	
		

	@Override
	public HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> getLocalCollectors() {
		HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> collectors = new HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>();
		collectors.put("PREFIXSUPPORT", (DesqProjDbDataCollector<?, ?>) new PrefixSupportCollector());
		collectors.put("TOTAL_EVENT_COUNT", (DesqProjDbDataCollector<?,?>) new EventsCountCollector());
		collectors.put("FST_STATES", (DesqProjDbDataCollector<?,?>) new FstStateItemCollector());
		collectors.put("PROJ_DB_FREQUENCIES", (DesqProjDbDataCollector<?,?>) new ProjDatabaseFrequencyCollector());
		collectors.put("MAX_REMAIN_TRANSACTION_LENGTH", (DesqProjDbDataCollector<?,?>) new MaxRemainingTransactionLengthCollector());
		return collectors;
	}

	@Override
	public double getScore(int[] prefix, HashMap<String,? extends DesqProjDbDataCollector<?,?>> statCollectors, int support) {
		double totalInformationGain = 0;
		EventsCountCollector sup = (EventsCountCollector) statCollectors.get("TOTAL_EVENT_COUNT");
		
		@SuppressWarnings("unchecked")
		Function<EventsCountCollector, Integer> func = (Function<EventsCountCollector, Integer>) statCollectors.get("TOTAL_EVENT_COUNT").finisher();
		
		int eventsCount = func.apply(sup);
		
		Int2IntOpenHashMap projDBItemFrequencies = (Int2IntOpenHashMap) statCollectors.get("PROJ_DB_FREQUENCIES");

		for (int i = 0; i < prefix.length; i++) {
			double itemInformationGain = 0;
			itemInformationGain = -1 * (Math.log((double) projDBItemFrequencies.get(prefix[i]) / ((double) eventsCount)) / Math.log(projDBItemFrequencies.size()));
			
			if(Double.isInfinite(itemInformationGain)) {
				System.out.println(projDBItemFrequencies.get(prefix[i]));
				System.out.println((double) eventsCount);
				System.out.println(Math.log(projDBItemFrequencies.size()));
				System.out.println("INFINITE");
			}
			totalInformationGain = totalInformationGain + itemInformationGain;
		}
//		if(support > 1) {
			return totalInformationGain;
//		} else {
//			return 0;
//		}
		
	}
}
