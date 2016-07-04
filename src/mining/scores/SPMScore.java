package mining.scores;

import java.util.HashMap;

import mining.statistics.DesqCountCollector;

public interface SPMScore {

	public double getScore(int[] prefix, HashMap<String, DesqCountCollector<?,?>> statCollectors, int support);
	
	public double getMaximumScore(int[] items,  HashMap<String, DesqCountCollector<?,?>> statCollectors);
	
	public double getItemScore(int item);
	
	public HashMap<String, DesqCountCollector<DesqCountCollector<?, ?>, ?>> getLocalCollectors();
}
