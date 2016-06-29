package mining.scores;

import java.util.HashMap;
import java.util.stream.Collector;

import mining.statistics.old.SPMLocalStatisticCollector;
import tools.FstGraph;


public abstract class DesqBaseScore implements SPMScore {

	@Override
	public double getScore(int[] prefix, HashMap<String, ?> statCollectors, int support) {
		return Double.MAX_VALUE;
	}

	@Override
	public double getMaximumScore(int[] items, HashMap<String, ?> statCollectors) {
		return Double.MAX_VALUE;
	}

	@Override
	public double getItemScore(int item) {
		return Double.MAX_VALUE;
	}
}
