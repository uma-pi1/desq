package mining.scores;

import java.util.HashMap;
import java.util.stream.Collector;

public interface SPMScore {

	public double getScore(int[] prefix, HashMap<String, ?> statCollectors, int support);
	
	public double getMaximumScore(int[] items,  HashMap<String, ?> statCollectors);
	
	public double getItemScore(int item);
	
	public HashMap<String, Collector> getLocalCollectors();
}
