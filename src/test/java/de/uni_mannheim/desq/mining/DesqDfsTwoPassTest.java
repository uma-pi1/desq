package de.uni_mannheim.desq.mining;

import org.apache.commons.configuration2.Configuration;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Created by rgemulla on 16.7.2016.
 */
@RunWith(Parameterized.class)
public class DesqDfsTwoPassTest extends Icdm16TraditionalMiningTest {
    public DesqDfsTwoPassTest(long sigma, int gamma, int lambda, boolean generalize) {
        super(sigma, gamma, lambda, generalize);
    }

    @Override
    public Configuration createConf() {
        String patternExpression = DesqMiner.patternExpressionFor(gamma, lambda, generalize);
        Configuration conf = DesqDfs.createConf(patternExpression, sigma);
        conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
        conf.setProperty("desq.mining.use.two.pass", true);
        return conf;
    }
}
