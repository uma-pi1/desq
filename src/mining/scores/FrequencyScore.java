package mining.scores;

import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collector;

import mining.statistics.DesqCountCollector;
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
	public double getScore(int[] prefix, HashMap<String, DesqCountCollector<?,?>> statData, int support) {
		return support;
	}

	@SuppressWarnings("unchecked")
	@Override
	public double getMaximumScore(int[] items, HashMap<String, DesqCountCollector<?,?>> statData) {
		PrefixSupportCollector sup = (PrefixSupportCollector) statData.get("PREFIXSUPPORT");
		Function<PrefixSupportCollector, Integer> func = (Function<PrefixSupportCollector, Integer>) statData.get("PREFIXSUPPORT").finisher();
		return func.apply(sup);
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public HashMap<String, DesqCountCollector<DesqCountCollector<?, ?>, ?>> getLocalCollectors() {
		HashMap<String, DesqCountCollector<DesqCountCollector<?, ?>, ?>> collectors = new HashMap<String, DesqCountCollector<DesqCountCollector<?, ?>,?>>();
		collectors.put("PREFIXSUPPORT", (DesqCountCollector) new PrefixSupportCollector());
		return collectors;
	}

}
