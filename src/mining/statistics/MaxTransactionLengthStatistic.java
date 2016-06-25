package mining.statistics;

public class MaxTransactionLengthStatistic implements SPMStatisticsAggregator<Integer,Integer> {

	@Override
	public Integer initialize(int transactionId, int[] transaction, int position, int pFSTState) {
		// update the support for the local item
		return transaction.length - position;
	}

	@Override
	public Integer merge(Integer p1, Integer p2) {
		return Integer.max(p1, p2);
	}

	@Override
	public Integer terminate(Integer p) {
		return p;
	}
	
}
