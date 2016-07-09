package mining.scores;

import java.util.HashMap;

import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.collectors.DesqResultDataCollector;

public interface DesqScore {
	public double getScoreBySequence(int[] sequence, 
									HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors);
	
//	public double getScoreByProjDb(int[] sequence, 
//									HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
//									HashMap<String,? extends DesqProjDbDataCollector<?,?>> finalStateProjDbCollectors,
//									HashMap<String,? extends DesqProjDbDataCollector<?,?>>[] prefixProjDbCollectors);
	
	public double getScoreByResultSet(int[] sequence,
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
			HashMap<String,? extends DesqResultDataCollector<?,?>> resultDataCollectors);
	
//	public double getScoreByResultSet(int[] sequence,
//									HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
//									HashMap<String,? extends DesqResultDataCollector<?,?>> resultDataCollectors,
//									HashMap<String,? extends DesqProjDbDataCollector<?,?>> finalStateProjDbCollectors,		
//									HashMap<String,? extends DesqProjDbDataCollector<?,?>>[] projDbCollectors);
	
	public HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?,?>, ?>> getGlobalDataCollectors();
	
	public HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?,?>, ?>> getProjDbCollectors();
	
	public HashMap<String, DesqResultDataCollector<? extends DesqResultDataCollector<?,?>, ?>> getResultDataCollectors();
	
	public double getMaxScoreByItem(int item, 
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors);
}
