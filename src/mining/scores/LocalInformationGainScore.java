package mining.scores;

import java.util.HashMap;
import java.util.stream.Collector;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import mining.statistics.DesqCountCollector;
import mining.statistics.EventsCountCollector;
import mining.statistics.FstStateItemCollector;
import mining.statistics.MaxRemainingTransactionLengthCollector;
import mining.statistics.PrefixSupportCollector;
import mining.statistics.ProjDatabaseFrequencyCollector;

public class LocalInformationGainScore extends DesqBaseScore implements SPMScore {	
		
	@Override
	public HashMap<String, DesqCountCollector<DesqCountCollector<?, ?>, ?>> getLocalCollectors() {
		HashMap<String, DesqCountCollector<?,?>> collectors = new HashMap<String, DesqCountCollector<?,?>>();
		collectors.put("PREFIXSUPPORT", new PrefixSupportCollector());
		collectors.put("TOTAL_EVENT_COUNT", new EventsCountCollector());
		collectors.put("FST_STATES", new FstStateItemCollector());
		collectors.put("PROJ_DB_FREQUENCIES", new ProjDatabaseFrequencyCollector());
		collectors.put("MAX_REMAIN_TRANSACTION_LENGTH", new MaxRemainingTransactionLengthCollector());
		return collectors;
	}

	@Override
	public double getScore(int[] prefix, HashMap<String, ?> statCollectors, int support){
		double totalInformationGain = 0;
		int eventsCount = (Integer) statCollectors.get("TOTAL_EVENT_COUNT");
//		int prefixSupport = (Integer) statCollectors.get("PREFIXSUPPORT");
		@SuppressWarnings("unchecked")
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
