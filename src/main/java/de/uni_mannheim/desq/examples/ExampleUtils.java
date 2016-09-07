package de.uni_mannheim.desq.examples;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.CountPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.Pattern;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;

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
        System.out.println(ConfigurationConverter.getProperties(ctx.conf));

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
    public static DesqMiner runWithStats(SequenceReader dataReader, Configuration minerConf) throws IOException {
        // create context
        DesqMinerContext ctx = new DesqMinerContext();
        ctx.dict = dataReader.getDictionary();
        CountPatternWriter result = new CountPatternWriter();
        ctx.patternWriter = result;
        ctx.conf = minerConf;

        // perform the de.uni_mannheim.desq.old.mining
        DesqMiner miner = ExampleUtils.runMiner(dataReader, ctx);

        // print results
        System.out.println("Number of patterns: " + result.getCount());
        System.out.println("Total frequency of all patterns: " + result.getTotalFrequency());
        return miner;
    }

    /**
     * Creates a miner, runs it on the given data, and prints running times as well as all mined patterns.
     */
    public static DesqMiner runVerbose(SequenceReader dataReader, Configuration minerConf) throws IOException {
        // create context
        DesqMinerContext ctx = new DesqMinerContext();
        ctx.dict = dataReader.getDictionary();
        MemoryPatternWriter result = new MemoryPatternWriter();
        ctx.patternWriter = result;
        ctx.conf = minerConf;

        // perform the de.uni_mannheim.desq.old.mining
        DesqMiner miner = ExampleUtils.runMiner(dataReader, ctx);

        // print results
        System.out.println("Patterns:");
        for (Pattern pattern : result.getPatterns()) {
            System.out.print(pattern.getFrequency());
            System.out.print(": ");
            System.out.println(dataReader.getDictionary().getItemsByFids(pattern.getItemFids()));
        }

        return miner;

    }

    /** Runs a miner on NYT data. */
    public static DesqMiner runNyt(Configuration minerConf) throws IOException {
        Dictionary dict = DictionaryIO.loadFromDel(new FileInputStream("data-local/nyt-1991-dict.del"), true);
        File dataFile = new File("data-local/nyt-1991-data.del");
        SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), true);
        dataReader.setDictionary(dict);
        return runWithStats(dataReader, minerConf);
    }

    /** Runs a miner on ICDM16 example data. */
    public static DesqMiner runIcdm16(Configuration minerConf) throws IOException {
        URL dictFile = ExampleUtils.class.getResource("/icdm16-example/dict.del");
        URL dataFile = ExampleUtils.class.getResource("/icdm16-example/data.del");

        // load the dictionary
        Dictionary dict = DictionaryIO.loadFromDel(dictFile.openStream(), false);

        // update hierarchy
        SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dict.incCounts(dataReader);
        dict.recomputeFids();
        System.out.println("Dictionary with statitics:");
        DictionaryIO.saveToDel(System.out, dict, true, true);
        System.out.println();

        // print sequences
        System.out.println("Input sequences:");
        dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dataReader.setDictionary(dict);
        IntList inputSequence = new IntArrayList();
        while (dataReader.readAsFids(inputSequence)) {
            System.out.println(dict.getItemsByFids(inputSequence));
        }
        System.out.println();

        dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dataReader.setDictionary(dict);
        return runVerbose(dataReader, minerConf);
    }
}