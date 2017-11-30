package de.uni_mannheim.desq.experiments;

import de.uni_mannheim.desq.mining.WeightedSequence;
import de.uni_mannheim.desq.mining.spark.DesqDataset;
import de.uni_mannheim.desq.experiments.MetricLogger.Metric;
import java.lang.reflect.Method;

public class PerformanceEvaluator {
    private Class desqMinerClass;
    private Method createConfMethod;
    private MetricLogger log;
    private DesqDataset data;//private String dataPath;
    private String logFile;
    //private String dictPath;

    public PerformanceEvaluator(Class desqMinerClass, DesqDataset data, String logFile){
        this.desqMinerClass = desqMinerClass;
        this.data = data;
        this.logFile = logFile;
        //this.dataPath = dataPath;
        //this.dictPath = dictPath;

        //get createCong method
        try {
            this.createConfMethod = desqMinerClass.getMethod("createConf", String.class, long.class);
        }catch (NoSuchMethodException e) {
            e.printStackTrace();
        }

    }

    public void run(int iterations){
        MetricLogger.reset(); //Force re-init of logger
        log = MetricLogger.getInstance();
        for(int i = 0; i < iterations; i++) {
            System.out.println("\n == Iteration " + i + " ==" );
            log.startIteration();
            data.sequences().unpersist(true);
            runIteration();
        }

        //Output results
        if(logFile != null){
            log.writeResult(logFile);
        }
    }

    private void runIteration(){

        // ------- Actual Processing  -------------
        log.start(Metric.TotalRuntime);

        //Convert Data?
        System.out.print("Loading data (" + data.context().getString("desq.dataset.builder.factory.class","No Builder") + ") ... ");
        log.start(Metric.DataTransformationRuntime);
        //cache data
        //Gather data (via scala/spark)
        //WeightedSequence[] cachedSequences = data.sequences().collect();
        System.out.println(log.stop(Metric.DataTransformationRuntime));


        //Convert PatEx


        // -------- Execute mining ------------
        log.start(Metric.PersistRuntime);


        System.out.println("PersistRuntime: " + log.stop(Metric.PersistRuntime));
        System.out.println("TotalRuntime: " + log.stop(Metric.TotalRuntime));



    }
}
