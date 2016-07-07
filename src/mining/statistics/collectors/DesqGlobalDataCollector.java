package mining.statistics.collectors;

import java.util.stream.Collector;

import mining.statistics.data.DesqTransactionData;

public interface DesqGlobalDataCollector<A extends DesqGlobalDataCollector<?,?>, R> extends Collector<DesqTransactionData, A, R> {

}
