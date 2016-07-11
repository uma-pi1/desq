package mining.scores;

import java.util.HashMap;

import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.collectors.DesqResultDataCollector;

public interface DesqScore {
	public double getScoreByResultSet(int[] sequence,
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
			HashMap<String,? extends DesqResultDataCollector<?,?>> resultDataCollectors);
	
	public HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?,?>, ?>> getGlobalDataCollectors();
	
	public HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?,?>, ?>> getProjDbCollectors();
	
	public HashMap<String, DesqResultDataCollector<? extends DesqResultDataCollector<?,?>, ?>> getResultDataCollectors();
	
	public double getMaxScoreByItem(int item, 
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors);
}
