package mining.statistics;

public class PrefixDocFrequencyStatistic implements SPMLocalStatisticCollector {
	
	int support;
	int previousTransactionId;
	
	public PrefixDocFrequencyStatistic() {
		support = 0;
		previousTransactionId = -1;
	}
	
	@Override
	public void onObservedItem(int transactionId, int[] transaction, int position, int pFSTState) {
		// update the support for the local item
		if(transactionId != previousTransactionId) {
			support++;
		}
		previousTransactionId = transactionId;
	}
	
	public int getFrequency() {
		return support;
	}
}
