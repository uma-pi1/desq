package de.uni_mannheim.desq.experiments.fimi;

import de.uni_mannheim.desq.examples.ExampleUtils;
import de.uni_mannheim.desq.mining.DesqCount;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.util.DesqProperties;

import java.io.IOException;

public class EvaluatePerfOnFimi {

    public enum Miner {DesqCount, DesqDfs}

    private static final String retail_itemset_data = "data-local/fimi_retail/retail.dat";
    private static final String retail_itemset_dict = "data-local/fimi_retail/dict.json";
    private static final String retail_seqOfItemsets_data = "data-local/fimi_retail/retail_sequences.dat";
    private static final String retail_seqOfItemsets_dict = "data-local/fimi_retail/dict.json";
    private static final String click_itemset_data = "data-local/fimi_click/kosarak.dat";
    private static final String click_seqOfItemsets_data = "data-local/fimi_click/kosarak_sequences.dat";

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
                        "(-/){1,3} [(/) (-/){1,3}]{1,2}",
                        5000),
                click_seqOfItemsets_data,
                null,
                "/",
                "data-local/log/Fimi_Seq_" + miner + "_",
                10,
                50,true, false, true
        );
    }

    private static void runFimi(Miner miner) throws IOException{

        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "A B (.){1,5}",
                        100),
                "data-local/fimi_retail/retail.dat",
                "data-local/fimi_retail/dict.json",
                null,
                "data-local/log/Fimi_" + miner + "_",
                10,
                10, true, true, false
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
                "data-local/log/ItemsetEx_" + miner + "_",
                10,
                10,true, true, true
        );
    }

    public static void main(String[] args) throws IOException{
        //runItemsetExample(Miner.DesqCount);

        //runFimi(Miner.DesqCount);

        runSequentialFimi(Miner.DesqDfs);
    }
}
