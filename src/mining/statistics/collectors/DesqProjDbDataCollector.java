package mining.statistics.collectors;

import java.util.stream.Collector;

import mining.statistics.data.ProjDbStatData;

public interface DesqProjDbDataCollector<A extends DesqProjDbDataCollector<?,?>, R> extends Collector<ProjDbStatData, A, R> {

}
