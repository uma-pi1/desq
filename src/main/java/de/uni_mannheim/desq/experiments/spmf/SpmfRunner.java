package de.uni_mannheim.desq.experiments.spmf;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.examples.ExampleUtils;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.util.DesqProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by ryan on 23.02.17.
 */
public class SpmfRunner {
    public static void implementSpmf(DesqProperties minerConf, SequenceReader dataReader) throws IOException {

        //ExampleUtils.runWithStats(dataReader, minerConf);
        ExampleUtils.runVerbose(dataReader, minerConf);

    }


    public static void setSpmf(String dataPath, String dictPath,String[] multipleSupp,int loop) throws IOException {
        //String patternExp= "[.*[(item)item*]+(@)]+[.*[(item)item*]+]|[.*[(item)item*]+]";
        String patternExp= "[.*[(movie)movie*]+(@)]+[.*[(movie)movie*]+]";


        File dataFile=new File(dataPath);
        File dictFile=new File(dictPath);



        Dictionary dict=Dictionary.loadFrom(dictFile);


        int i;
        for(int j=0;j<loop;j++){
            for(i=0;i<multipleSupp.length;i++){
                System.out.println("Support Count "+multipleSupp[i]);

                //setting the configuration
                DesqProperties conf = DesqDfs.createConf(patternExp, Integer.parseInt(multipleSupp[i]));
                conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
                conf.setProperty("desq.mining.use.two.pass", true);

                SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), false);
                dataReader.setDictionary(dict);
                //implementing the algorithm
                implementSpmf(conf, dataReader);

                System.out.println("\n");
            }
        }

    }

    /** Run: java filename dataPath dictPath min_supp [looping factor]*/
    public static void main(String[] args) throws IOException {
        //String dataPath="data/spmf/ibm/ibm1.del";
        //String dictPath="data/spmf/ibm/ibm1.json";
        //int sigma=50;
        //int sigma=Integer.parseInt(args[2]);
        //checking for valid set of argumnets
        if(args.length<2){
            System.out.println("Please enter valid set of arguments");
            System.out.println(".del .json minsup [looping factor]");
            throw new IllegalArgumentException();
        }else if(args.length>4){
            System.out.println("Too many arguments");
            System.out.println("Please enter valid set of arguments");
            System.out.println(".del .json minsup [looping factor]");
            throw new IllegalArgumentException();
        }
        String dataPath=args[0];
        String dictPath=args[1];
        String[] sigma=args[2].trim().split(" ");
        int loop;
        if(args.length==4){
            loop=Integer.parseInt(args[3]);
        }else{
            loop=1;
        }


        setSpmf(dataPath,dictPath,sigma,loop);
    }
}
