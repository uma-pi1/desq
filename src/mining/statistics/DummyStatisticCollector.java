package mining.statistics;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class DummyStatisticCollector implements SPMLocalStatisticCollector, Collector {
	@Override
	public void onObservedItem(int transactionId, int[] transaction, int position, int pFSTState) {
		// simply do nothing
	}

	@Override
	public BiConsumer accumulator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set characteristics() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BinaryOperator combiner() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Function finisher() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Supplier supplier() {
		// TODO Auto-generated method stub
		return null;
	}
}
