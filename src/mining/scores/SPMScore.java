package mining.scores;

import mining.statistics.SPMLocalStatisticCollector;

public interface SPMScore {

	public double getScore(int[] prefix, SPMLocalStatisticCollector[] statisticCollector);
	
	public double getMaximumScore(int[] items, int support, SPMLocalStatisticCollector[] sequenceStatistics);
	
//	public double getItemScore(int item);
}
