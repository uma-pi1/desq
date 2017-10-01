package de.uni_mannheim.desq.examples;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.CountPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.WeightedSequence;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Created by rgemulla on 01.09.2016.
 */
public class ExampleUtils {
    /**
     * Creates a miner, runs it on the given data, and prints running times.
     */
    public static DesqMiner runMiner(SequenceReader dataReader, DesqMinerContext ctx) throws IOException {
        System.out.print("Miner properties: ");
        System.out.println(ctx.conf.toProperties());

        System.out.print("Creating miner... ");
        Stopwatch prepTime = Stopwatch.createStarted();
        DesqMiner miner = DesqMiner.create(ctx);
        prepTime.stop();
        System.out.println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms");

        System.out.print("Reading input sequences... ");
        Stopwatch ioTime = Stopwatch.createStarted();
        miner.addInputSequences(dataReader);
        ioTime.stop();
        System.out.println(ioTime.elapsed(TimeUnit.MILLISECONDS) + "ms");


        System.out.print("Mining... ");
        Stopwatch miningTime = Stopwatch.createStarted();
        miner.mine();
        miningTime.stop();
        System.out.println(miningTime.elapsed(TimeUnit.MILLISECONDS) + "ms");

        System.out.println("Total time: " +
                (prepTime.elapsed(TimeUnit.MILLISECONDS) + ioTime.elapsed(TimeUnit.MILLISECONDS)
                        + miningTime.elapsed(TimeUnit.MILLISECONDS)) + "ms");

        return miner;
    }

    /**
     * Creates a miner, runs it on the given data, and prints running times as well as pattern statistics.
     */
    public static DesqMiner runWithStats(SequenceReader dataReader, DesqProperties minerConf) throws IOException {
        // create context
        DesqMinerContext ctx = new DesqMinerContext();
        ctx.dict = dataReader.getDictionary();
        CountPatternWriter result = new CountPatternWriter();
        ctx.patternWriter = result;
        ctx.conf = minerConf;

        // perform the mining
        DesqMiner miner = ExampleUtils.runMiner(dataReader, ctx);

        // print results
        System.out.println("Number of patterns: " + result.getCount());
        System.out.println("Total frequency of all patterns: " + result.getTotalFrequency());
        return miner;
    }

    /**
     * Creates a miner, runs it on the given data, and prints running times as well as all mined patterns.
     */
    public static DesqMiner runVerbose(SequenceReader dataReader, DesqProperties minerConf) throws IOException {
        // create context
        DesqMinerContext ctx = new DesqMinerContext();
        ctx.dict = dataReader.getDictionary();
        MemoryPatternWriter result = new MemoryPatternWriter();
        ctx.patternWriter = result;
        ctx.conf = minerConf;

        // perform the mining
        DesqMiner miner = ExampleUtils.runMiner(dataReader, ctx);

        // print results
        System.out.println("Patterns:");
        for (WeightedSequence pattern : result.getPatterns()) {
            System.out.print(pattern.weight);
            System.out.print(": ");
            System.out.println(dataReader.getDictionary().sidsOfFids(pattern));
        }

        return miner;

    }

    /** Runs a miner on NYT data. */
    public static DesqMiner runNyt(DesqProperties minerConf) throws IOException {
        Dictionary dict = Dictionary.loadFrom("data/icdm16-example/nyt-1991-dict.avro.gz");
        File dataFile = new File("data/icdm16-example/nyt-1991-data.del");
        SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), true);
        dataReader.setDictionary(dict);
        return runWithStats(dataReader, minerConf);
    }

    /** Runs a miner on Netflix data */
    public static DesqMiner runNetflixFlat(DesqProperties minerConf) throws IOException {
        Dictionary dict = Dictionary.loadFrom("data-local/netflix/flat-dict.avro.gz");
        dict.recomputeFids();
        File dataFile = new File("data-local/netflix/flat-data-gid.del");
        SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), false);
        dataReader.setDictionary(dict);
        return runVerbose(dataReader, minerConf);
    }

    /** Runs a miner on Netflix data */
    public static DesqMiner runNetflixDeep(DesqProperties minerConf) throws IOException {
        Dictionary dict = Dictionary.loadFrom("data-local/netflix/deep-dict.avro.gz");
        dict.recomputeFids();
        File dataFile = new File("data-local/netflix/deep-data-gid.del");
        SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), false);
        dataReader.setDictionary(dict);
        return runVerbose(dataReader, minerConf);
    }


    /** Runs a miner on Proteins data */
    public static DesqMiner runProtein(DesqProperties minerConf) throws IOException {
        Dictionary dict = Dictionary.loadFrom("data/icdm16-example/protein-dict.avro.gz");
        dict.recomputeFids();
        dict.freeze();
        File dataFile = new File("data/icdm16-example/protein-data-gid.del");
        SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), false);
        dataReader.setDictionary(dict);
        return runVerbose(dataReader, minerConf);
    }



    /** Runs a miner on ICDM16 example data. */
    public static DesqMiner runIcdm16(DesqProperties minerConf) throws IOException {
        URL dictFile = ExampleUtils.class.getResource("/icdm16-example/dict.json");
        URL dataFile = ExampleUtils.class.getResource("/icdm16-example/data.del");

        // load the dictionary
        Dictionary dict = Dictionary.loadFrom(dictFile);

        // update hierarchy
        SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dict.incFreqs(dataReader);
        dict.recomputeFids();
        System.out.println("Dictionary with statitics:");
        dict.writeJson(System.out);
        System.out.println();

        // print sequences
        System.out.println("Input sequences:");
        dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dataReader.setDictionary(dict);
        IntList inputSequence = new IntArrayList();
        while (dataReader.readAsFids(inputSequence)) {
            System.out.println(dict.sidsOfFids(inputSequence));
        }
        System.out.println();

        dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dataReader.setDictionary(dict);
        return runVerbose(dataReader, minerConf);
    }
}