package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.util.TestUtils;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for traditional frequent sequence mining. Datasets and parameters are set by implementing classes.
 *
 * Created by rgemulla on 18.07.2016.
 */
@RunWith(Parameterized.class)
public abstract class TraditionalMiningTest {
    private static Logger logger = Logger.getLogger(TraditionalMiningTest.class);

    long sigma;
    int gamma, lambda;
    boolean generalize;

    TraditionalMiningTest(long sigma, int gamma, int lambda, boolean generalize) {
        this.sigma = sigma;
        this.gamma = gamma;
        this.lambda = lambda;
        this.generalize = generalize;
    }

    /** The dictionary to use */
    public abstract Dictionary getDictionary() throws IOException;

    /** Whether counts and fids need to be computed from data */
    public abstract boolean computeStatisticsAndFids();

    /** Reader for data */
    public abstract SequenceReader getSequenceReader() throws IOException;

    /** Base file name for expected outputs */
    public abstract String getBaseFileName();

    /** Class of miner */
    public abstract Class<? extends DesqMiner> getMinerClass();

    /** Properties for miner */
    public abstract Properties createProperties();

    @Test
    public void test() throws IOException,
            InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Class<? extends DesqMiner> minerClass = getMinerClass();
        String fileName = getBaseFileName() + "-" + sigma + "-" + gamma + "-" + lambda + "-" + generalize + ".del";
        File actualFile = TestUtils.newTemporaryFile(
                TestUtils.getPackageResourcesPath(getClass()) + "/" + fileName);
        mine(actualFile);
        try {
            File expectedFile = TestUtils.getPackageResource(getClass(), fileName);
            assertThat(actualFile).hasSameContentAs(expectedFile);
        } catch (NullPointerException e) {
            logger.error("Can't access expected data file for " + actualFile);
            throw e;
        }
    }

    public void mine(File outputDelFile)
            throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
                InstantiationException {
        // load the dictionary
        Dictionary dict = getDictionary();

        // update hierarchy
        if (computeStatisticsAndFids()) {
            SequenceReader dataReader = getSequenceReader();
            dataReader.setDictionary(dict);
            dict.clearCounts();
            dict.incCounts(dataReader);
            dataReader.close();
            dict.recomputeFids();
        }

        // Perform pattern mining into del file
        SequenceReader dataReader = getSequenceReader();
        dataReader.setDictionary(dict);
        DesqMinerContext ctx = new DesqMinerContext();
        ctx.dict = dict;
        DelPatternWriter patternWriter = new DelPatternWriter(new FileOutputStream(outputDelFile), true);
        patternWriter.setDictionary(dict);
        ctx.patternWriter = patternWriter;
        ctx.properties = createProperties();
        DesqMiner miner = getMinerClass().getConstructor(DesqMinerContext.class).newInstance(ctx);
        miner.addInputSequences(dataReader);
        dataReader.close();
        miner.mine();
        patternWriter.close();

        // sort del file
        TestUtils.sortDelPatternFile(outputDelFile);
    }
}
