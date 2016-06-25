package mining.scores;

import java.util.HashMap;
import java.util.stream.Collector;

import mining.statistics.GlobalItemDocFrequencyStatistic;
import mining.statistics.PrefixSupportCollector;

public class FrequencyScore implements SPMScore {
	GlobalItemDocFrequencyStatistic globalItemFrequencyStatistic;
	
	PrefixSupportCollector prefixFrequencyStatistic;
	
	int minFrequency;
	
	public FrequencyScore(GlobalItemDocFrequencyStatistic globalItemFrequencyStatistic) {
//		super(fstGraph);
//		this.minFrequency = minFrequency;
		this.globalItemFrequencyStatistic = globalItemFrequencyStatistic;
	}

//	@Override
//	public boolean isItemRelevant(int item) {
//		return globalItemFrequencyStatistic.getFrequency(item) >= minFrequency;
//	}

//	@Override
//	public double getScore(double score) {
//		return score >= minFrequency;
//	}

//	@Override
//	public double getScore(int[] prefix, SPMLocalStatisticCollector[] statisticCollector) {
//		return ((PrefixDocFrequencyStatistic) statisticCollector[statisticCollector.length]).getFrequency() >= minFrequency;
//	}
//
//	@Override
//	public double getMaximumScore(int[] items, int support, SPMLocalStatisticCollector[] sequenceStatistics) {
//		return support;
//	}

	public double getItemScore(int item) {
		return globalItemFrequencyStatistic.getFrequency(item);
	}

	@Override
	public double getScore(int[] prefix, HashMap<String, ?> statData) {
		return (Integer) statData.get("PREFIXSUPPORT");
	}

	@Override
	public double getMaximumScore(int[] items, HashMap<String, ?> statData) {
		return (Integer) statData.get("PREFIXSUPPORT");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public HashMap<String, Collector> getLocalCollectors() {
		HashMap<String, Collector> collectors = new HashMap<String, Collector>();
		collectors.put("PREFIXSUPPORT", new PrefixSupportCollector());
		return collectors;
	}

}
