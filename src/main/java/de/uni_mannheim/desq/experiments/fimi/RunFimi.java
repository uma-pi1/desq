package de.uni_mannheim.desq.experiments.fimi;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.examples.ExampleUtils;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.util.DesqProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

/**
 * Created by ryan on 15.02.17.
 */
public class RunFimi {

    public static void implementFimi(DesqProperties minerConf, String dataPath, String dictPath) throws IOException {
        File dataFile=new File(dataPath);
        File dictFile=new File(dictPath);

        SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), false);

        Dictionary dict=Dictionary.loadFrom(dictFile);
        dataReader.setDictionary(dict);
        ExampleUtils.runWithStats(dataReader, minerConf);

    }

    public static void setFimi(String dataPath, String dictPath) throws IOException {
        String patternExp= "[.*(.)]+";
        int sigma =50;

        DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
        conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
        conf.setProperty("desq.mining.use.two.pass", true);
        implementFimi(conf, dataPath, dictPath);
    }

    public static void main(String[] args) throws IOException {
        String dataPath="data/fimi/retail/test.del";
        String dictPath="data/fimi/retail/test-dict.json";
        setFimi(dataPath,dictPath);
    }
}
