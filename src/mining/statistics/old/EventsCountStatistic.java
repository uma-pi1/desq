package mining.statistics.old;

public class EventsCountStatistic implements SPMStatisticsAggregator<Integer,Integer> {
	
	int eventsCount;
	int previousTransactionId;
	
//	@Override
//	public Integer initialize() {
//		eventsCount = 0;
//		return 0;
//	}
//
//	@Override
//	public Integer merge(Integer p1, Integer p2) {
//		return p1 + p2;
//	}
//
//	@Override
//	public Integer terminate(Integer p) {
//		return eventsCount;
//	}
//
//	@Override
//	public void accumulate(int transactionId, int[] transaction, int position, int pFSTState) {
//		if(transactionId != previousTransactionId) {
//			eventsCount = eventsCount + transaction.length - position; 
//		}
//	}
}
