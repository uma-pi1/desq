package mining.scores;

import java.util.HashMap;

import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.collectors.DesqResultDataCollector;
import fst.XFst;

public class DesqBaseScore implements DesqScore,
										DesqCountScore,
										DesqDfsScore {
	
	protected XFst xfst;
	
	public DesqBaseScore(XFst xfst) {
		this.xfst = xfst;
	}

//	@Override
//	public double getScoreBySequence(
//			int[] sequence,
//			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors) {
//		throw new NotImplementedExcepetion();
//	}

	@Override
	public double getScoreByProjDb(int[] sequence, 
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>> finalStateProjDbCollectors,
			HashMap<String,? extends DesqProjDbDataCollector<?,?>>[] prefixProjDbCollectors) {
		throw new NotImplementedExcepetion();
	}

	@Override
	public double getScoreByResultSet(int[] sequence,
			HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors,
			HashMap<String,? extends DesqResultDataCollector<?,?>> resultDataCollectors) {
		throw new NotImplementedExcepetion();
	}

	@Override
	public HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> getGlobalDataCollectors() {
		throw new NotImplementedExcepetion();
	}

	@Override
	public HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> getProjDbCollectors() {
		throw new NotImplementedExcepetion();
	}

	@Override
	public HashMap<String, DesqResultDataCollector<? extends DesqResultDataCollector<?, ?>, ?>> getResultDataCollectors() {
		throw new NotImplementedExcepetion();
	}

	@Override
	public double getMaxScoreByPrefix(
			int[] prefix,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors,
			HashMap<String, ? extends DesqProjDbDataCollector<?, ?>>[] prefixProjDbCollectors) {
		return Double.MAX_VALUE;
	}

	@Override
	public double getMaxScoreByPrefix(
			int[] prefix,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollector,
			int[] transaction,
			int transactionId,
			int position,
			int fstState,
			int[] transCount) {
		return Double.MAX_VALUE;
	}
	
	@Override
	public double getMaxScoreByItem(
			int item,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors) {
		return Double.MAX_VALUE;
	}

	@Override
	public double getScoreByProjDb(
			int[] sequence,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors,
			HashMap<String, ? extends DesqProjDbDataCollector<?, ?>> finalStateProjDbCollectors) {
		throw new NotImplementedExcepetion();
	}

	@Override
	public double getScoreBySequence(
			int[] sequence,
			HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors) {
		throw new NotImplementedExcepetion();
	}
}
