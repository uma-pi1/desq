package mining.scores;

import java.util.HashMap;

import mining.statistics.collectors.DesqGlobalDataCollector;

public interface DesqCountScore extends DesqScore {

	public double getMaxScoreByPrefix(int[] prefix,  HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollector);

}
