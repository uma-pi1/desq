package mining.statistics.collectors;

import java.util.stream.Collector;

import mining.statistics.data.DesqSequenceData;

public interface DesqResultDataCollector<A extends DesqResultDataCollector<?,?>, R> extends Collector<DesqSequenceData, A, R> {

}