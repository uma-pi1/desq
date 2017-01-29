package de.uni_mannheim.desq.experiments.fimi;

import de.uni_mannheim.desq.converters.Fimi.FimiConverter;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.examples.ExampleUtils;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.net.URL;

/**
 * Created by ryan on 29.01.17.
 */
public class Fimi {


    public static void  runFimi(DesqProperties minerConf, String dataPath) throws IOException {
        Dictionary dict= FimiConverter.loadFrom(dataPath);

        URL dataFile = ExampleUtils.class.getResource("/sortedFimiRepo/sort.del");
        // load the dictionary


        // update hierarchy
        SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dict.incFreqs(dataReader);
        dict.recomputeFids();
        System.out.println("Dictionary with statitics:");
        //dict.writeJson(System.out);
        System.out.println();

        //print sequences
        System.out.println("Input sequences:");
        dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dataReader.setDictionary(dict);

        IntList inputSequence = new IntArrayList();
/*
        while (dataReader.readAsFids(inputSequence)) {
            System.out.println(dict.sidsOfFids(inputSequence));
        }
        System.out.println();
*/
        dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dataReader.setDictionary(dict);
        //return runVerbose(dataReader, minerConf);
        ExampleUtils.runVerbose(dataReader, minerConf);

    }

    public static void fimi(String dataPath) throws IOException {
        String patternExp= "(.)[.*(.)]*";
        int sigma =1000;

        DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
        conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
        conf.setProperty("desq.mining.use.two.pass", true);
        runFimi(conf, dataPath);
    }

    public static void main(String[] args) throws IOException {
        String dataPath="/home/ryan/DESQ/desq-journal/desq-desqOnFimi-bdf66e7a3dc785bcb9eb8dd16138d6e2bd421c48/data/fimiRepo/retail.dat";
        fimi(dataPath);
    }
}
