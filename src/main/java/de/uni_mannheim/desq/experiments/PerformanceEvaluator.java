package de.uni_mannheim.desq.experiments;

import de.uni_mannheim.desq.Desq;
import de.uni_mannheim.desq.dictionary.BuilderFactory;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.spark.DesqDataset;
import de.uni_mannheim.desq.experiments.MetricLogger.Metric;
import de.uni_mannheim.desq.patex.PatExTranslator;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.io.IOException;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.atomic.LongAdder;

public class PerformanceEvaluator {
    private DesqProperties desqMinerConfigTemplate;
    //private DesqDataset data;
    private String dataPath;
    private BuilderFactory factory;
    private PatExTranslator<String> patExTranslator;
    private String logFile;
    private MetricLogger log;
    private SparkConf conf;

    public PerformanceEvaluator(DesqProperties desqMinerConfig, String dataPath, BuilderFactory factory,
                                String logFile, PatExTranslator<String> patExTranslator){
        this.desqMinerConfigTemplate = desqMinerConfig;
        this.dataPath = dataPath;
        this.factory = factory;
        //this.data = data;
        if(logFile != null){
            this.logFile = logFile;
        }
        if(patExTranslator != null){
            this.patExTranslator = patExTranslator;
        }

        //init Spark
        this.conf = new SparkConf().setAppName(getClass().getName()).setMaster("local");
        Desq.initDesq(conf);

    }

    public void run(int iterations){
        MetricLogger.reset(); //Force re-init of logger
        log = MetricLogger.getInstance();
        for(int i = 0; i < iterations; i++) {
            System.out.println("\n == Iteration " + i + " ==" );
            log.startIteration();
            runIteration();
        }
        //Output results (optional)
        if(logFile != null){
            log.writeResult(logFile);
        }
    }

    private void runIteration(){
        //Copy config to ensure changes are not passed to next iteration
        DesqProperties minerConf = new DesqProperties(desqMinerConfigTemplate);
        SparkContext sc = SparkContext.getOrCreate(conf);

        // ------- Processing  -------------
        log.start(Metric.TotalRuntime);

        // ---- Load Data
        System.out.print("Loading data (" + factory.getProperties().getString("desq.dataset.builder.factory.class","No Builder") + ") ... ");
        log.start(Metric.DataLoadRuntime);
        // Init data load via DesqDataset (lazy) via Spark
        DesqDataset data = DesqDataset.loadDesqDatasetForJava(sc, dataPath, factory);

        //Gather data (via scala/spark)
        List<String[]> cachedSequences = data.toSids().toJavaRDD().collect();
        System.out.println(log.stop(Metric.DataLoadRuntime));

        // ---- Calculating some KPIs (impact on total runtime only)
        System.out.println("#Dictionary entries: " + log.add(Metric.NumberDictionaryItems, data.dict().size()));


        System.out.println("#Input sequences: "
                + log.add(Metric.NumberInputSequences, cachedSequences.size()));

        //Sum up the length of each sequence
        LongAdder adder = new LongAdder();
        cachedSequences.parallelStream().forEach(seq -> adder.add(seq.length));
        int sum = adder.intValue();

        System.out.println("Avg length of input sequences: "
                + log.add(Metric.AvgLengthInputSequences, sum/cachedSequences.size()));

        // ---- Convert PatEx (optional)
        System.out.print("Converting PatEx ( " + (patExTranslator != null) + " )... ");
        log.start(Metric.PatExTransformationRuntime);

        String patEx = minerConf.getString("desq.mining.pattern.expression");
        System.out.print( patEx );
        if(patExTranslator != null){
            patEx = patExTranslator.translate();
            minerConf.setProperty("desq.mining.pattern.expression", patEx);
            System.out.print("  ->  " + patEx);
            //new PatExToSequentialPatEx(itemsetPatEx).translate()
        }
        //new PatExToSequentialPatEx(patEx).translate()
        System.out.println(" ... " + log.stop(Metric.PatExTransformationRuntime));


        // -------- Execute mining ------------
        log.start(Metric.MiningRuntime);

        //Prep
        log.start(Metric.MiningPrepRuntime);
        DesqMinerContext ctx = new DesqMinerContext(minerConf, data.dict());
        MemoryPatternWriter result = new MemoryPatternWriter();
        ctx.patternWriter = result;
        DesqMiner miner = DesqMiner.create(ctx);

        SequenceReader seqReader = new StringIteratorSequenceReader(data.dict(), cachedSequences.iterator());
        log.stop(Metric.MiningPrepRuntime);

        //Read Input
        log.start(Metric.MiningReadRuntime);
        try { miner.addInputSequences(seqReader);}
        catch (IOException e) { e.printStackTrace();}
        log.stop(Metric.MiningReadRuntime);

        //Exec mining
        log.start(Metric.MiningMineRuntime);
        miner.mine();
        log.stop(Metric.MiningMineRuntime);

        System.out.println("MiningRuntime: " + log.stop(Metric.MiningRuntime));

        // ------ Collect Result KPIs ------------
        System.out.println("#Result Patterns: " + log.add(Metric.NumberResultPatterns,result.size()));

        System.out.println("TotalRuntime: " + log.stop(Metric.TotalRuntime));

        //Cleanup Spark (used for data load via DesqDataset)
        sc.stop();
    }

    private class StringIteratorSequenceReader extends SequenceReader{
        private Iterator<String[]> it;

        private StringIteratorSequenceReader(Dictionary dict, Iterator<String[]> iterator){
            this.dict = dict;
            this.it = iterator;
        }

        @Override
        public boolean read(IntList items) throws IOException {
            if (it.hasNext()) {
                String[] tokens = it.next();
                items.size(tokens.length);
                items.clear();
                for(String s: tokens){
                    items.add(dict.fidOf(s));
                }
                return true;
            }
            return false;
        }

        @Override
        public boolean usesFids() {
            //Convert all into Fids (used anyways later)
            return true;
        }

        @Override
        public void close() throws IOException {
            //Nothing to close
        }
    }
}
