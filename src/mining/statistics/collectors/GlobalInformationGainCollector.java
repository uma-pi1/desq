package mining.statistics.collectors;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import mining.statistics.data.DesqTransactionData;

public class GlobalInformationGainCollector implements  DesqGlobalDataCollector<GlobalInformationGainCollector, double[]>,
													Supplier<GlobalInformationGainCollector>,
													BiConsumer<GlobalInformationGainCollector, DesqTransactionData> { 
	private int numberOfEvents;
	private int[] flist;
	private double[] gainList;
	

	
	public double getInformationGain(int item) {		
		return gainList[item];
	}
	
	private double[] calculateInformationGainIndex(int[] flist, int events) {
		gainList = new double[flist.length];
		
		for (int i = 0; i < flist.length; i++) {
			gainList[i] = -1 * (Math.log(((double)flist[i]) / ((double) events)) / Math.log(flist.length));
		}
		
		return gainList;
	}

	@Override
	public Supplier<GlobalInformationGainCollector> supplier() {
	  return GlobalInformationGainCollector::new;
	}

	// Collector Method
	@Override
	public BiConsumer<GlobalInformationGainCollector, DesqTransactionData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<GlobalInformationGainCollector> combiner() {
		return (acc1,acc2) -> acc1;
	}

	// Collector Method
	@Override
	public Function<GlobalInformationGainCollector, double[]> finisher() {
		return (acc) -> {
			return calculateInformationGainIndex(flist, numberOfEvents);
		};
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public GlobalInformationGainCollector get() {
		return new GlobalInformationGainCollector();
	}

	
	// BiConsumer Method
	@Override
	public void accept(GlobalInformationGainCollector t, DesqTransactionData u) {
		t.numberOfEvents = t.numberOfEvents + u.getTransaction().length;
	}
}
