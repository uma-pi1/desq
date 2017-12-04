package de.uni_mannheim.desq.experiments.fimi;

import de.uni_mannheim.desq.examples.ExampleUtils;
import de.uni_mannheim.desq.mining.DesqCount;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.util.DesqProperties;

import java.io.IOException;

public class EvaluatePerfOnFimi {

    public enum Miner {DesqCount, DesqDfs}
    private static DesqProperties getMinerConf(Miner miner, String patEx, long sigma){
        DesqProperties conf;
        switch (miner){
            case DesqCount:
                conf = DesqCount.createConf(patEx, sigma); break;
            case DesqDfs:
                conf = DesqDfs.createConf(patEx, sigma); break;
            default: throw new UnsupportedOperationException("Unsupported Miner");
        }

        conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
        conf.setProperty("desq.mining.use.two.pass", false);
        conf.setProperty("desq.mining.optimize.permutations", false);

        return conf;
    }

    private static void runSequentialFimi(Miner miner) throws IOException{

        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "(-/) /{1,2} (-/)",
                        100),
                "data-local/fimi_retail/retail_sequences.dat",
                "data-local/fimi_retail/dict.json",
                "/",
                "data-local/Fimi_Seq_",
                2
        );
    }

    private static void runFimi(Miner miner) throws IOException{

        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "(.){2,5}",
                        5000),
                "data-local/fimi_retail/retail.dat",
                "data-local/fimi_retail/dict.json",
                null,
                "data-local/Fimi_",
                10
        );
    }

    public static void runItemsetExample(Miner miner) throws IOException{
        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "[c|d] / (B)",
                        1),
                "data/itemset-example/data.dat",
                "data/itemset-example/dict.json",
                "/",
                "data-local/ItemsetEx_",
                10
        );
    }

    public static void main(String[] args) throws IOException{
        //runItemsetExample(Miner.DesqCount);

        //runFimi(Miner.DesqDfs);

        runSequentialFimi(Miner.DesqCount);
    }
}
