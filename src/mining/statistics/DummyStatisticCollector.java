package mining.statistics;


public class DummyStatisticCollector implements SPMStatisticsAggregator {
//	@Override
//	public void onObservedItem(int transactionId, int[] transaction, int position, int pFSTState) {
//		// simply do nothing
//	}

	@Override
	public Object initialize(int transactionId, int[] transaction, int position, int pFSTState) {
		// TODO Auto-generated method stub
		
		PrefixSupportCollector sum = new PrefixSupportCollector();
		
		sum.accumulator(1,2);
		return null;
	}

	@Override
	public Object merge(Object p1, Object p2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object terminate(Object p) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void accumulate(int transactionId, int[] transaction, int position,
			int pFSTState) {
		// TODO Auto-generated method stub
		
	}

}
