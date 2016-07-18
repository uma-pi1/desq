package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.util.TestUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Properties;

/**
 * Created by rgemulla on 18.07.2016.
 */
public class MinerTestUtils {
    public static void mineIcdm16Example(Class<? extends DesqMiner> minerClass, File outputDelFile,
                                         Properties properties)
            throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
                InstantiationException {
        // load the dictionary
        URL dictFile = MinerTestUtils.class.getResource("/icdm16-example/dict.del");
        URL dataFile = MinerTestUtils.class.getResource("/icdm16-example/data.del");
        Dictionary dict = DictionaryIO.loadFromDel(dictFile.openStream(), false);

        // update hierarchy
        SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dict.incCounts(dataReader);
        dataReader.close();
        dict.recomputeFids();

        // Perform pattern mining into del file
        dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dataReader.setDictionary(dict);
        DesqMinerContext ctx = new DesqMinerContext();
        ctx.dict = dict;
        DelPatternWriter patternWriter = new DelPatternWriter(new FileOutputStream(outputDelFile), true);
        patternWriter.setDictionary(dict);
        ctx.patternWriter = patternWriter;
        ctx.properties = new Properties(properties);
        DesqMiner miner = minerClass.getConstructor(DesqMinerContext.class).newInstance(ctx);
        miner.addInputSequences(dataReader);
        dataReader.close();
        miner.mine();
        patternWriter.close();

        // sort del file
        TestUtils.sortDelPatternFile(outputDelFile);
    }
}
