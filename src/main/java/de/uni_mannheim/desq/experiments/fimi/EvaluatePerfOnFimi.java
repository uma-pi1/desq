package de.uni_mannheim.desq.experiments.fimi;

import de.uni_mannheim.desq.examples.ExampleUtils;
import de.uni_mannheim.desq.mining.*;
import de.uni_mannheim.desq.mining.spark.DesqDataset;
import de.uni_mannheim.desq.util.DesqProperties;

import java.io.IOException;

public class EvaluatePerfOnFimi {

    public enum Miner {DesqCount, DesqDfs,DesqDfs_twoPass, DesqDfsPatricia, DesqDfsPatriciaIndex}

    private static final String retail_itemset_data = "data-local/fimi_retail/retail.dat";
    private static final String retail_itemset_dict = "data-local/fimi_retail/dict.json";
    private static final String retail_seqOfItemsets_data = "data-local/fimi_retail/retail_sequences.dat";
    private static final String retail_seqOfItemsets_dict = "data-local/fimi_retail/dict.json";
    private static final String click_itemset_data = "data-local/fimi/kosarak/kosarak.dat";//"data-local/fimi_click/kosarak.dat";
    private static final String click_seqOfItemsets_data = "data-local/fimi_click/kosarak_sequences.dat";
    private static final String nyt91_desqDataset = "data-local/nyt-1991-data/desqDataset";
    private static final String nyt_annotated_data = "data-local/nyt/nyt-data-gid.del";
    private static final String nyt_annotated_dict = "data-local/nyt/nyt-dict.json";
    private static final String amazon_itemset_desqdataset = "data-local/amazon_itemset";
    private static final String amazon_data_gid = "data-local/amazon/amazon-data-gid.del";
    private static final String amazon_data_fid = "data-local/amazon/amazon-data-fid.del";
    private static final String amazon_dict = "data-local/amazon/amazon-dict.json";
    private static final String amazon_dict_avro = "data-local/amazon/amazon-dict.avro.gz";

    private static DesqProperties getMinerConf(Miner miner, String patEx, long sigma){
        DesqProperties conf;
        switch (miner){
            case DesqCount:
                conf = DesqCount.createConf(patEx, sigma);
                conf.setProperty("desq.mining.use.two.pass", false); // has to be set for DesqCount due to bug
                break;
            case DesqDfs:
                conf = DesqDfs.createConf(patEx, sigma);
                conf.setProperty("desq.mining.use.two.pass", false);
                break;
            case DesqDfs_twoPass:
                conf = DesqDfs.createConf(patEx, sigma);
                conf.setProperty("desq.mining.use.two.pass", true);
                break;
            case DesqDfsPatricia:
                conf = DesqDfsPatricia.createConf(patEx, sigma);
                break;
            case DesqDfsPatriciaIndex:
                conf = DesqDfsPatriciaIndex.createConf(patEx, sigma);
                break;
            default: throw new UnsupportedOperationException("Unsupported Miner");
        }

        conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
        conf.setProperty("desq.mining.use.lazy.dfa", true);

        conf.setProperty("desq.mining.optimize.permutations", true);

        return conf;
    }

    private static void runSequentialFimi(Miner miner) throws IOException{

        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "(-/){1,3} [(/) (-/){1,3}]{1,2}",
                        5000),
                click_seqOfItemsets_data,
                null,
                false,
                false,
                false,
                "/",
                "data-local/log/Fimi_Seq_" + miner + "_",
                1,
                50,
                false, false, true,
                true
        );
    }

    //private static void runFimi(Miner miner, int n) throws IOException{
    private static void runFimi(Miner miner) throws IOException{

        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        //"A B (.){1," + n + "}",//"(.){1," + n + "}",//"A B (.){1," + n + "}", //measured for permute
                        //"(.){" + n + "}",//"A B (.){" + n + "}", //measured for permute
                        "(.){2,4}", //"A (.){2,4}" "6 (.){2,4}"  //used for algorithm comparison
                        //"A&(.)!{2}",
                        //"A B (.){1,3}", //"A B 30 1198 (.)", "A B (.){1,3}", "A B (.){1,5}"
                        //"(.)",//"(A).{1,5}$",
                        5000),
                //retail_itemset_data, retail_itemset_dict, //sigma 10
                click_itemset_data, null, //sigma 5000
                false,
                false,
                false,
                null,
                "data-local/log/Fimi_Click_Simple_Metric_" + miner + "_",
                11,
                0,
                false, true, false,
                true
        );
    }

    private static void runNyt(Miner miner) throws IOException {
        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "(.)!{3}&.!{0,2}",  //"(.)!{3}&.!*",
                        100000),
                nyt_annotated_data, nyt_annotated_dict,
                false,
                true,
                true,
                null,
                "data-local/log/NytAn_" + miner + "_",
                11,
                0,
                true, false, false,
                true
        );
    }

    private static void runNyt91(Miner miner) throws IOException {
        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "(.)!{3}&.!{0,2}",  //"(.)!{3}&.!*",
                        100),
                nyt91_desqDataset, null,
                false,
                false,
                true,
                null,
                "data-local/log/Nyt91_" + miner + "_",
                11,
                0,
                true, false, false,
                true
        );
    }

    private static void runAmazon(Miner miner) throws IOException {
        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "(.)!{2,4}&.!*",
                        1000),
                amazon_data_fid, amazon_dict_avro,
                false,
                true,
                true,
                null,
                "data-local/log/Amazon_" + miner + "_",
                1,
                30,
                true, false, false,
                true
        );
    }

    private static void runAmazonItemset(Miner miner) throws IOException {
        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "(Books@){2,4}",
                        1000),
                amazon_itemset_desqdataset, null,
                false,
                false,
                true,
                null,
                "data-local/log/AmazonItemset_Books_" + miner + "_",
                11,
                0,
                false, true, false,
                true
        );
    }

    private static void runAmazonItemset2(Miner miner) throws IOException {
        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "Books@ (.){2,4}",
                        1000),
                amazon_itemset_desqdataset, null,
                false,
                false,
                true,
                null,
                "data-local/log/AmazonItemset_ConstraintBooks_" + miner + "_",
                11,
                0,
                false, true, false,
                true
        );
    }

    private static void runAmazonItemset3(Miner miner) throws IOException {
        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "(.){2,4}",
                        1000),
                amazon_itemset_desqdataset, null,
                false,
                false,
                true,
                null,
                "data-local/log/AmazonItemset_Simple_Metrics_" + miner + "_",
                11,
                0,
                false, true, false,
                true
        );
    }

    public static void runItemsetExample(Miner miner) throws IOException{
        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "[c|d] / (B)",
                        1),
                "data/itemset-example/data.dat",
                "data/itemset-example/dict.json",
                false,
                false,
                false,
                "/",
                "data-local/log/ItemsetEx_" + miner + "_",
                10,
                10,
                true, true, true,
                true
        );
    }

    public static void runIcdm16(Miner miner) throws IOException{
        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        //"[c|d] (.){1,3} A",
                        //"(.){1,3}",
                        //"(A^)*",//"(A) e", //"(A^)",
                        //"(a1)..",//"[A.* (.)]|[(.).* A]", // [A.*]&[(.).*]!{1,3}
                        //"[(a2).*]* (a1).* [(a2).*]*",//"(c) a1+ $ ",//"A.*(B)",
                        "[c|d|e] (A)!*&(B)+&a1!+ [d|e]",
                        //"c a1 $",
                        1),
                "data/icdm16-example/data.del",
                "data/icdm16-example/dict.json",
                true,
                true,
                false,
                "/",
                "data-local/log/icdm16Ex_" + miner + "_",
                1,
                10,
                true, false, false,
                true
        );
    }

    public static void main(String[] args) throws IOException{
        //runItemsetExample(Miner.DesqCount);

        //runFimi(Miner.DesqCount);
        //runFimi(Miner.DesqDfs);
        //runFimi(Miner.DesqDfs_twoPass);
        //runFimi(Miner.DesqDfsPatricia);
        //runFimi(Miner.DesqDfsPatriciaIndex);
        //runSequentialFimi(Miner.DesqDfs);

        //runIcdm16(Miner.DesqCount);
        runIcdm16(Miner.DesqDfs);
        //runIcdm16(Miner.DesqDfsPatricia);

        //Nyt Annotated
        //runNyt(Miner.DesqDfs);
        //runNyt(Miner.DesqDfsPatricia); // memory usage explodes during trie build!

        //runNyt91(Miner.DesqDfs);

        //Amazon
        //runAmazon(Miner.DesqDfs);
        //runAmazon(Miner.DesqDfsPatricia);

        //runAmazonItemset(Miner.DesqDfs);
        //runAmazonItemset(Miner.DesqDfs_twoPass);
        //runAmazonItemset(Miner.DesqDfsPatricia);
        //runAmazonItemset(Miner.DesqDfsPatriciaIndex);

        //runAmazonItemset2(Miner.DesqDfs);
        //runAmazonItemset2(Miner.DesqDfs_twoPass);
        //runAmazonItemset2(Miner.DesqDfsPatricia); // ABORT
        //runAmazonItemset2(Miner.DesqDfsPatriciaIndex);

        //runAmazonItemset3(Miner.DesqDfs);
        //runAmazonItemset3(Miner.DesqDfs_twoPass);
        //runAmazonItemset3(Miner.DesqDfsPatricia);
        //runAmazonItemset3(Miner.DesqDfsPatriciaIndex);



        //for(int i = 1; i < 11 ;i++) runFimi(Miner.DesqDfs_twoPass, i);
    }
}
