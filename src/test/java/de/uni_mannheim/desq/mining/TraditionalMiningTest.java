package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.util.DesqProperties;
import de.uni_mannheim.desq.util.TestUtils;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for traditional frequent sequence de.uni_mannheim.desq.old.mining. Datasets and parameters are set by implementing classes.
 *
 * Created by rgemulla on 18.07.2016.
 */
@RunWith(Parameterized.class)
public abstract class TraditionalMiningTest {
    private static final Logger logger = Logger.getLogger(TraditionalMiningTest.class);

    long sigma;
    int gamma, lambda;
    boolean generalize;
    String minerName;
    DesqProperties conf;

    TraditionalMiningTest(long sigma, int gamma, int lambda, boolean generalize,
                          String minerName, DesqProperties conf) {
        this.sigma = sigma;
        this.gamma = gamma;
        this.lambda = lambda;
        this.generalize = generalize;
        this.minerName = minerName;
        this.conf = conf;
    }

    /** The dictionary to use */
    public abstract Dictionary getDictionary() throws IOException;

    /** Whether counts and fids need to be computed from data */
    public abstract boolean computeStatisticsAndFids();

    /** Reader for data */
    public abstract SequenceReader getSequenceReader() throws IOException;

    public abstract String getGoldFileBaseName();

    public abstract String getTestDirectoryName();

    @Test
    public void test() throws IOException,
            InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        String fileName = getGoldFileBaseName() + "-" + sigma + "-" + gamma + "-" + lambda + "-" + generalize + ".del";
        File actualFile = TestUtils.newTemporaryFile(
                TestUtils.getPackageResourcesPath(getClass()) + "/" + getTestDirectoryName() + "/" + minerName + "/" + fileName);
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
            dict.clearCountsAndFids();
            dict.incCounts(dataReader);
            dataReader.close();
            dict.recomputeFids();
        }

        // Perform pattern de.uni_mannheim.desq.old.mining into del file
        SequenceReader dataReader = getSequenceReader();
        dataReader.setDictionary(dict);
        DesqMinerContext ctx = new DesqMinerContext();
        ctx.dict = dict;
        DelPatternWriter patternWriter = new DelPatternWriter(new FileOutputStream(outputDelFile), DelPatternWriter.TYPE.GID);
        patternWriter.setDictionary(dict);
        ctx.patternWriter = patternWriter;
        ctx.conf = conf;
        DesqMiner miner = DesqMiner.create(ctx);
        miner.addInputSequences(dataReader);
        dataReader.close();
        miner.mine();
        patternWriter.close();

        // sort del file
        TestUtils.sortDelPatternFile(outputDelFile);
    }
}
