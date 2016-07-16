package mining.statistics.old;

import java.util.stream.Collector;

import mining.statistics.collectors.PrefixSupportCollector;

public class SPMLocalStatisticFactory {
	
	public static enum StatisticType {
		DUMMY, PREFIX_SUPPORT, LOCAL_ITEM_FREQUENCY , MAX_TRANSACTION_LENGTH,FST_STATE_STATISTIC
	}
	
	private static StatisticType type;
	
	public static Collector<?, ?, ?> createInstance() {
		switch (type) {
		case PREFIX_SUPPORT:
			return new PrefixSupportCollector();
//		case LOCAL_ITEM_FREQUENCY:
//			return new LocalItemFrequencyStatistic();
//		case MAX_TRANSACTION_LENGTH:
//			return new MaxTransactionLengthStatistic();
//		case FST_STATE_STATISTIC:
//			return new FSTStateStatistic();
		default:
//			return new DummyStatisticCollector();
			return null;
		}
	}
	
	public static void setStatisticType(StatisticType statisticType) {
		type = statisticType;
	}
	
}
