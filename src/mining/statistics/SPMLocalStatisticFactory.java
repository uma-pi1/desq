package mining.statistics;

public class SPMLocalStatisticFactory {
	
	public static enum StatisticType {
		DUMMY, DOC_FREQUENCY, TRANSACTION_STATE_ITEM 	
	}
	
	private static StatisticType type;
	
	public static SPMLocalStatisticCollector createInstance() {
		switch (type) {
		case DOC_FREQUENCY:
			return new PrefixDocFrequencyStatistic();
		case TRANSACTION_STATE_ITEM:
			return new TransactionStateItemStatistic();
		default:
			return new DummyStatisticCollector();
		}
	}
	
	public static void setStatisticType(StatisticType statisticType) {
		type = statisticType;
	}
	
}
