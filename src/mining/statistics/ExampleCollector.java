package mining.statistics;

import java.util.stream.Collector;

public class ExampleCollector {
	@SuppressWarnings(value = { "rawtypes", "unchecked" })
	public static void main(String[] args) {
		Collector sum = new PrefixSupportCollector();
		SPMStatisticsData data = new SPMStatisticsData();
		Object x = sum.supplier().get();
		Object y = sum.supplier().get();
		
		sum.accumulator().accept(x, data);
		sum.accumulator().accept(y, data);
		
		x = sum.combiner().apply(x, y);
		
		Integer b = (Integer) sum.finisher().apply(x);
		
		System.out.println("Test " + b);
	}

}
