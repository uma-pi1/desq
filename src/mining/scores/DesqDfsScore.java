package mining.scores;

import java.util.HashMap;

import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;

public interface DesqDfsScore extends DesqScore {
	
	public double getMaxScoreByPrefix(int[] prefix,  
						HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
						HashMap<String,? extends DesqProjDbDataCollector<?,?>> projDbCollectors);
	
}