package mining.statistics;

import java.util.stream.Collector;

public interface DesqCountCollector<A extends DesqCountCollector<?,?>, R> extends Collector<SPMStatisticsData, A, R> {

}
