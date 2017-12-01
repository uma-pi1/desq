package de.uni_mannheim.desq.experiments;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricLogger {
    private static MetricLogger instance;
    private int currentIteration;
    private Map<Metric,Map<Integer, Long>> metrics;

    public enum Metric { //Define metrics and their order
        //Parameters
        StartTimestamp,
        NumberDictionaryItems, NumberInputSequences, AvgLengthInputSequences,
        NumberPatExItems, NumberDistinctPatExItems, NumberResultPatterns,
        //Runtime Metrics
        DataLoadRuntime, PatExTransformationRuntime,
        RDDConstructionRuntime,
        MiningRuntime, MiningPrepRuntime, MiningReadRuntime, MiningMineRuntime,
        FstGenerationRuntime, FstGenerationParseTreeRuntime, FstGenerationWalkRuntime, FstMinimizationRuntime,
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
        this.currentIteration = -1;
        this.metrics = new HashMap<>();
        //Init iteration lists
        for(Metric m: Metric.values()){
            metrics.put(m,new HashMap<>());
        }
    }

    //reset everything
    public static void reset(){
        instance = null;
    }

    //next iteration
    public void startIteration(){
        this.currentIteration++;
        metrics.get(Metric.StartTimestamp).put(currentIteration, Instant.now().getEpochSecond());
    }

    // Time measurements
    public void start(Metric metric){
        metrics.get(metric).put(currentIteration,System.nanoTime());
    }

    public String stop(Metric metric){
        long time = TimeUnit.NANOSECONDS.toMillis(
                System.nanoTime() - metrics.get(metric).get(currentIteration));
        metrics.get(metric).put(currentIteration, time);
        return time  + "ms";
    }

    // Other numeric metrics
    public Long add(Metric metric, Long value){
        metrics.get(metric).put(currentIteration,value);
        return value;
    }

    public int add(Metric metric, int value){
        metrics.get(metric).put(currentIteration, (long) value);
        return value;
    }

    public void writeResult(String file){
        writeResult(file,";");
    }

    public void writeResult(String file, String separator){
        StringBuilder headerBuilder = new StringBuilder();
        headerBuilder.append("Metric");
        for(int i = 0; i <= currentIteration; i++){
            headerBuilder.append(separator);
            headerBuilder.append("Iteration");
            headerBuilder.append(i);
        }

        //Construct lines
        List<String> lines = new ArrayList<>();
        lines.add(headerBuilder.toString());
        //collect data
        for(Metric m: Metric.values()){
            StringBuilder lineBuilder = new StringBuilder();
            lineBuilder.append(m.toString());
            Map<Integer,Long> iterations = metrics.get(m);

            for(int i = 0; i <= currentIteration; i++){
                lineBuilder.append(separator);
                if(iterations.containsKey(i)){
                    lineBuilder.append(iterations.get(i));
                }else{
                    lineBuilder.append("-");
                }
            }
            lines.add(lineBuilder.toString());
        }

        //Save the data
        Path path = Paths.get(file);
        try {
            Files.write(path, lines, Charset.forName("UTF-8"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        }catch (IOException ex){
           System.out.println("Exception: " + ex.getLocalizedMessage());
        }

    }
}
