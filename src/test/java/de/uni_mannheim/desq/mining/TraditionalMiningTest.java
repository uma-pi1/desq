package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.util.TestUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for traditional frequent sequence mining. Datasets and parameters are set by implementing classes.
 *
 * Created by rgemulla on 18.07.2016.
 */
@RunWith(Parameterized.class)
public abstract class TraditionalMiningTest {
    private static final Logger logger = Logger.getLogger(TraditionalMiningTest.class);

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

    /** Properties for miner */
    public abstract Configuration createConf();

    @Test
    public void test() throws IOException,
            InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        String fileName = getBaseFileName() + "-" + sigma + "-" + gamma + "-" + lambda + "-" + generalize + ".del";
        File actualFile = TestUtils.newTemporaryFile(
                TestUtils.getPackageResourcesPath(getClass()) + "/" + getClass().getSimpleName() + "/" + fileName);
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
        DelPatternWriter patternWriter = new DelPatternWriter(new FileOutputStream(outputDelFile), DelPatternWriter.TYPE.GID);
        patternWriter.setDictionary(dict);
        ctx.patternWriter = patternWriter;
        ctx.conf = createConf();
        DesqMiner miner = DesqMiner.create(ctx);
        miner.addInputSequences(dataReader);
        dataReader.close();
        miner.mine();
        patternWriter.close();

        // sort del file
        TestUtils.sortDelPatternFile(outputDelFile);
    }
}
