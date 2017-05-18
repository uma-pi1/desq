package de.uni_mannheim.desq.experiments.fimi;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.examples.ExampleUtils;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.util.DesqProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by ryan on 15.02.17.
 */
public class RunFimi {

    public static DesqMiner implementFimi(DesqProperties minerConf, String dataPath, String dictPath) throws IOException {
        File dataFile=new File(dataPath);
        File dictFile=new File(dictPath);

        SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), false);

        Dictionary dict=Dictionary.loadFrom(dictFile);
        dataReader.setDictionary(dict);
        //ExampleUtils.runWithStats(dataReader, minerConf);
        return ExampleUtils.runVerbose(dataReader,minerConf);

    }

    public static void setFimi(String dataPath, String dictPath) throws IOException {
        //String patternExp= "[.*(.)]+";
        String patternExp=".*(edge node <)[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(edge node)< [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >[edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < [ edge node < >]* >]* >]* >]* >]* >]* >]* >]* >]* >]* >]*(>)";
        int sigma =1;

        DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
        conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
        conf.setProperty("desq.mining.use.two.pass", true);
        implementFimi(conf, dataPath, dictPath);
    }

    /**argument1->del file  argument2->dictionary*/
    public static void main(String[] args) throws IOException {
        //String dataPath="data/fimi/retail/test.del";
        //String dictPath="data/fimi/retail/test-dict.json";
        String dataPath=args[0];
        String dictPath=args[1];
        setFimi(dataPath,dictPath);
    }
}
