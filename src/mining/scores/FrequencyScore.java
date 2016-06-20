package mining.scores;

import mining.statistics.GlobalItemDocFrequencyStatistic;
import mining.statistics.PrefixDocFrequencyStatistic;
import mining.statistics.SPMLocalStatisticCollector;

public class FrequencyScore extends DesqBaseScore implements SPMScore {
	GlobalItemDocFrequencyStatistic globalItemFrequencyStatistic;
	
	PrefixDocFrequencyStatistic prefixFrequencyStatistic;
	
	int minFrequency;
	
	public FrequencyScore(int minFrequency, GlobalItemDocFrequencyStatistic globalItemFrequencyStatistic) {
//		super(fstGraph);
		this.minFrequency = minFrequency;
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

	@Override
	public double getScore(int[] prefix, SPMLocalStatisticCollector[] statisticCollector) {
		return ((PrefixDocFrequencyStatistic) statisticCollector[statisticCollector.length]).getFrequency() >= minFrequency;
	}

	@Override
	public double getMaximumScore(int[] items, int support, SPMLocalStatisticCollector[] sequenceStatistics) {
		return support;
	}

	public double getItemScore(int item) {
		return globalItemFrequencyStatistic.getFrequency(item);
	}

}
