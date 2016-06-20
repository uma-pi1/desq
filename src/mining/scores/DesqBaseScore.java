package mining.scores;

import mining.statistics.SPMLocalStatisticCollector;
import tools.FstGraph;


public abstract class DesqBaseScore implements SPMScore {
	
	FstGraph fstGraph;
	
	public DesqBaseScore() {
	}

	@Override
	public double getScore(int[] prefix, SPMLocalStatisticCollector[] statisticCollector) {
		return 0.0;
	}
	
	@Override
	public double getMaximumScore(int[] items, int support, SPMLocalStatisticCollector[] sequenceStatistics) {
		return 0.0;
	}
}
