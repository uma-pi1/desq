package de.uni_mannheim.desq.experiments;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricLogger {
    private static MetricLogger instance;
    private Map<Metric,Long> metrics;

    public enum Metric { //Define metrics and their order
        //Parameters
        NumberDictionaryItems, NumberInputSequences, AvgLengthInputSequences,
        NumberPatExItems, NumberDistinctPatExItems,
        //Runtime Metrics
        DataTransformationRuntime, PatExTransformationRuntime,
        RDDConstructionRuntime,
        PersistRuntime, FstGenerationRuntime, FstGenerationParseTreeRuntime, FstGenerationWalkRuntime, FstMinimizationRuntime,
        TotalRuntime
    }

    //Singleton concept
    public static MetricLogger getInstance(){
        if (instance == null){
            instance = new MetricLogger();
        }
        return instance;
    }

    private MetricLogger() {
        this.metrics = new HashMap<>();
    }

    //reset everything
    public void reset(){
        this.metrics = new HashMap<>();
    }

    // Time measurements
    public void start(Metric metric){
        metrics.put(metric, System.nanoTime());
    }


    public String stop(Metric metric){
        long time = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - metrics.get(metric));
        metrics.put(metric, time);
        return time  + "ms";
    }

    // Other numeric metrics
    public Long add(Metric metric, Long value){
        metrics.put(metric,value);
        return value;
    }
    /*public Integer add(Metric metric, Integer value){
        metrics.put(metric,value.longValue());
        return value;
    }*/


    public void writeResult(String file){
        String header = "Metric, Value";
        List<String> lines = new ArrayList<>();

        lines.add(header);
        //collect data
        for(Metric m: Metric.values()){
            if(metrics.containsKey(m)) {
                lines.add(m.toString() + "," + metrics.get(m).toString());
            }
        }

        Path path = Paths.get(file);
        try {
            Files.write(path, lines, Charset.forName("UTF-8"), StandardOpenOption.CREATE);
        }catch (IOException ex){
           System.out.println("Exception: " + ex.getLocalizedMessage());
        }

    }
}
