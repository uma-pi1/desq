package de.uni_mannheim.desq.mining;

import org.apache.commons.configuration2.Configuration;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Created by rgemulla on 16.7.2016.
 */
@RunWith(Parameterized.class)
public class DesqDfsWithPruningIcdm16Test extends Icdm16TraditionalMiningTest {
    public DesqDfsWithPruningIcdm16Test(long sigma, int gamma, int lambda, boolean generalize) {
        super(sigma, gamma, lambda, generalize);
    }

    @Override
    public Configuration createConf() {
        String patternExpression = DesqMiner.patternExpressionFor(gamma, lambda, generalize);
        Configuration conf = DesqDfs.createConf(patternExpression, sigma);
        conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
        return conf;
    }
}
