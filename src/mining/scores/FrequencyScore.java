package mining.scores;

import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collector;

import fst.XFst;
import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.collectors.ItemSupportCollector;
import mining.statistics.collectors.PrefixSupportCollector;
import mining.statistics.old.GlobalItemDocFrequencyStatistic;

public class FrequencyScore extends DesqBaseScore {
	
	public FrequencyScore(XFst xfst) {
		super(xfst);
	}
	
	@Override
	public double getMaxScoreByItem(
			int item,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors) {
		
		ItemSupportCollector sup = (ItemSupportCollector) globalDataCollectors.get(ItemSupportCollector.ID);
		
		@SuppressWarnings("unchecked")
		Function<ItemSupportCollector, int[]> func = (Function<ItemSupportCollector, int[]>) globalDataCollectors.get(ItemSupportCollector.ID).finisher();		
		
		return func.apply(sup)[item]; 
	}
	
	@Override
	public double getScoreByProjDb(int[] sequence, 
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>> finalStateProjDbCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>>[] prefixProjDbCollectors) {

		return (getScoreByProjDb(sequence, globalDataCollectors, finalStateProjDbCollectors));
		
	}
	
	@Override
	public double getScoreByProjDb(
			int[] sequence,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors,
			HashMap<String, ? extends DesqProjDbDataCollector<?, ?>> finalStateProjDbCollectors) {
		
		PrefixSupportCollector sup = (PrefixSupportCollector) finalStateProjDbCollectors.get("PREFIXSUPPORT");
		@SuppressWarnings("unchecked")
		Function<PrefixSupportCollector, Integer> func = (Function<PrefixSupportCollector, Integer>) finalStateProjDbCollectors.get("PREFIXSUPPORT").finisher();

		return func.apply(sup);
	}

	@Override
	public double getMaxScoreByPrefix(
			int[] prefix,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors,
			HashMap<String, ? extends DesqProjDbDataCollector<?, ?>>[] prefixProjDbCollectors) {
		
		PrefixSupportCollector sup = (PrefixSupportCollector) prefixProjDbCollectors[prefixProjDbCollectors.length - 1].get("PREFIXSUPPORT");
		
		@SuppressWarnings("unchecked")
		Function<PrefixSupportCollector, Integer> func = (Function<PrefixSupportCollector, Integer>) prefixProjDbCollectors[prefixProjDbCollectors.length - 1].get("PREFIXSUPPORT").finisher();
		
		return func.apply(sup);
	}

	@Override
	public HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> getGlobalDataCollectors() {
		HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> globalDataCollectors = new HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>>();
		globalDataCollectors.put(ItemSupportCollector.ID, (DesqGlobalDataCollector<?,?>) new ItemSupportCollector());
		return globalDataCollectors;
	}

	@Override
	public HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> getProjDbCollectors() {
		HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> projDbCollectors = new HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>();
		projDbCollectors.put("PREFIXSUPPORT", (DesqProjDbDataCollector<?,?>) new PrefixSupportCollector());
		return projDbCollectors;
	}
}
