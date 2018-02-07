package de.uni_mannheim.desq.experiments;

import de.uni_mannheim.desq.Desq;
import de.uni_mannheim.desq.dictionary.BuilderFactory;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.examples.spark.ExampleUtils;
import de.uni_mannheim.desq.io.CountPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.WeightedSequence;
import de.uni_mannheim.desq.mining.spark.DesqDataset;
import de.uni_mannheim.desq.experiments.MetricLogger.Metric;
import de.uni_mannheim.desq.patex.PatExToSequentialPatEx;
import de.uni_mannheim.desq.patex.PatExTranslator;
import de.uni_mannheim.desq.util.DesqProperties;
import de.uni_mannheim.desq.util.Profiler;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.atomic.LongAdder;

public class PerformanceEvaluator {
    private static final boolean profileMemoryUsage = true;

    private DesqProperties desqMinerConfigTemplate;
    private String dataPath;
    private String dictPath; //only needed if no factory used
    private BuilderFactory factory;
    private PatExTranslator<String> patExTranslator;
    private String logFile;
    private MetricLogger log;
    private SparkConf conf;
    private int print = 0;
    private boolean usesGids;
    private boolean isDelFile;
    private boolean bypassBuilder;
    private DesqDataset data; //references the data of current iteration (might be null)
    private Dictionary dict; //references the dict of current iteration
    private SparkContext sc;

    public PerformanceEvaluator(DesqProperties desqMinerConfig,
                                String dataPath,
                                String dictPath,
                                Boolean usesGids,
                                Boolean isDelFile,
                                Boolean bypassBuilder,
                                BuilderFactory factory,
                                Integer print,
                                String logFile,
                                PatExTranslator<String> patExTranslator
    ){
        this.desqMinerConfigTemplate = desqMinerConfig;
        this.dataPath = dataPath;
        this.usesGids = usesGids;
        this.isDelFile = isDelFile;

        this.factory = factory;

        if(dictPath != null){
            this.dictPath = dictPath;
        }

        if(print != null) this.print = print;
        else this.print = 0;

        if(logFile != null) this.logFile = logFile;

        if(patExTranslator != null) this.patExTranslator = patExTranslator;

        if(bypassBuilder != null) this.bypassBuilder = bypassBuilder;
        else this.bypassBuilder = false;

        //init Spark
        if(!(this.bypassBuilder && this.dataPath.contains(".del"))) {
            this.conf = new SparkConf().setAppName(getClass().getName()).setMaster("local");
            Desq.initDesq(conf);
            conf.set("spark.kryoserializer.buffer.max", "1024m");
        }
    }

    public void run(int iterations) throws IOException{
        //Force re-init of logger
        MetricLogger.reset();
        //init spark (once per run)
        sc = (conf != null) ? SparkContext.getOrCreate(conf) : null;

        log = MetricLogger.getInstance();
        for(int i = 0; i < iterations; i++) {
            System.out.println("\n == Iteration " + i + " ==" );
            log.startIteration();
            MemoryPatternWriter result = runIteration();
            System.out.println("Sum of result pattern weights: " +
                    result.getPatterns().stream().mapToLong(ws -> ws.weight).sum());
            if(i == 0 && print > 0) {
                //Print up to x inputs and patterns
                if(data != null) {
                    System.out.println("Input data (up to " + print + "):");
                    data.print(print);
                }
                System.out.println("Result patterns (up to " + print + "):");
                int cnt = 0;
                for (WeightedSequence ws : result.getPatterns()) {
                    System.out.println(dict.sidsOfFids(ws) + "@" + ws.weight);
                    if ((cnt += 1) >= print) break;
                }
            }

            //Cleanup Spark (used for data load via DesqDataset)
            if(data != null) {
                data.broadcastDictionary().destroy();
                data = null;
            }
            result.close();

        }
        //Stop Spark after all iterations
        if(sc != null) sc.stop();


        //Output log in csv (optional)
        if(logFile != null){
            log.writeToFile(logFile);
        }
    }

    private MemoryPatternWriter runIteration() throws IOException{
        //Copy config to ensure changes are not passed to next iteration
        DesqProperties minerConf = new DesqProperties(desqMinerConfigTemplate);
        Profiler profiler = new Profiler();

        // ------- Processing  -------------
        log.start(Metric.TotalRuntime);

        // ---- Load Data
        log.start(Metric.DataLoadRuntime);
        SequenceReader seqReader;

        if(bypassBuilder && isDelFile){
            //Load data from a .del file and avro dictionary directly (no spark usage)
            System.out.print("Loading data (from files without builder) ... ");
            //Read avro dict
            dict = Dictionary.loadFrom(dictPath);
            dict.recomputeFids();
            seqReader = new DelSequenceReader(
                    dict,
                    new FileInputStream(new File(dataPath)),
                    !usesGids
            );
            System.out.println(log.stop(Metric.DataLoadRuntime));
        }else {
            // Init data load via DesqDataset (lazy) via Spark
            if(bypassBuilder){
                //Load persisted DesqDataset from a folder
                System.out.print("Loading data (from files without builder) ... ");
                data = DesqDataset.load(dataPath,sc);
                dict = data.dict();
            }else {
                //Load from Del file (if usesGids) or CSV (FIMI -> separated by space)
                System.out.print("Loading data (factory: " + factory.getClass().getCanonicalName() + ") ... ");
                data = (isDelFile)
                        ? ExampleUtils.buildDesqDatasetFromDelFile(sc, dataPath, factory)
                        : ExampleUtils.buildDesqDatasetFromRawFile(sc, dataPath, factory, " ");
                dict = data.dict();
            }

            //Gather data (via scala/spark)
            List<String[]> cachedSequences = data.toSids().toJavaRDD().collect();
            System.out.println(log.stop(Metric.DataLoadRuntime));

            //Determine Fid of itemset separator (easier analysis later)
            String itemsetSeparatorSid = data.properties().getString("desq.dataset.itemset.separator.sid", null);
            if (itemsetSeparatorSid != null) {
                System.out.println("Itemset Separator FId: " + dict.fidOf(itemsetSeparatorSid));
            }


            // ---- Calculating some KPIs (impact on total runtime only)
            System.out.println("#Dictionary entries: " + log.add(Metric.NumberDictionaryItems, dict.size()));


            System.out.println("#Input sequences: "
                    + log.add(Metric.NumberInputSequences, cachedSequences.size()));

            //Sum up the length of each sequence
            LongAdder adder = new LongAdder();
            cachedSequences.parallelStream().forEach(seq -> adder.add(seq.length));
            int sum = adder.intValue();

            System.out.println("Avg length of input sequences: "
                    + log.add(Metric.AvgLengthInputSequences, sum/cachedSequences.size()));

            seqReader = new StringIteratorSequenceReader(dict, cachedSequences.iterator());
        }
        // ---- Convert PatEx (optional)
        System.out.print("Converting PatEx ( " + (patExTranslator != null) + " )... ");
        log.start(Metric.PatExTransformationRuntime);

        String patEx = minerConf.getString("desq.mining.pattern.expression");
        System.out.print( patEx );
        if(patExTranslator != null){
            patEx = patExTranslator.translate();
            minerConf.setProperty("desq.mining.pattern.expression", patEx);
            System.out.print("  ->  " + patEx);
            /*patEx = new PatExToSequentialPatEx(patEx).translate();
            System.out.print("  ->  " + patEx);
            minerConf.setProperty("desq.mining.pattern.expression", patEx);*/
        }
        System.out.println(" ... " + log.stop(Metric.PatExTransformationRuntime));


        // -------- Execute mining ------------
        log.start(Metric.MiningRuntime);

        //Prep
        log.start(Metric.MiningPrepRuntime);
        System.out.print("Prep. Mining ... ");
        DesqMinerContext ctx = new DesqMinerContext(minerConf, dict);
        MemoryPatternWriter result = new MemoryPatternWriter();
        ctx.patternWriter = result;
        DesqMiner miner = DesqMiner.create(ctx);
        System.out.println(log.stop(Metric.MiningPrepRuntime));

        //Read Input
        System.out.print("Read ... ");
        if(profileMemoryUsage) profiler.start();
        log.start(Metric.MiningReadRuntime);
        try { miner.addInputSequences(seqReader);}
        catch (IOException e) { e.printStackTrace();}
        if(profileMemoryUsage) {
            profiler.stop();
            System.out.println(log.stop(Metric.MiningReadRuntime)
                    + " (Memory: " + log.add(Metric.MemoryForReading, profiler.usedMemory/1024) + "kB)");
        }else
            System.out.println(log.stop(Metric.MiningReadRuntime));


        //Exec mining
        System.out.print("Mine ... ");
        if(profileMemoryUsage) profiler.start();
        log.start(Metric.MiningMineRuntime);
        miner.mine();
        if(profileMemoryUsage) {
            profiler.stop();
            System.out.println(log.stop(Metric.MiningMineRuntime)
                    + " (Memory: " + log.add(Metric.MemoryForMining, profiler.usedMemory/1024) + "kB)");
        }else
            System.out.println(log.stop(Metric.MiningMineRuntime));

        System.out.println("Total Mining Time: " + log.stop(Metric.MiningRuntime));

        // ------ Collect Result KPIs ------------
        System.out.println("#Result Patterns: " + log.add(Metric.NumberResultPatterns,result.size()));

        System.out.println("TotalRuntime: " + log.stop(Metric.TotalRuntime));

        return result;
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
