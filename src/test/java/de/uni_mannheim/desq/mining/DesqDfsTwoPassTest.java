package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.util.PropertiesUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Properties;

/**
 * Created by rgemulla on 16.7.2016.
 */
@RunWith(Parameterized.class)
public class DesqDfsTwoPassTest extends Icdm16TraditionalMiningTest {
    public DesqDfsTwoPassTest(long sigma, int gamma, int lambda, boolean generalize) {
        super(sigma, gamma, lambda, generalize);
    }

    @Override
    public Class<? extends DesqMiner> getMinerClass() {
        return DesqDfs.class;
    }

    @Override
    public Properties createProperties() {
        String patternExpression = DesqMiner.patternExpressionFor(gamma, lambda, generalize);
        Properties properties = DesqDfs.createProperties(patternExpression, sigma);
        PropertiesUtils.set(properties, "pruneIrrelevantInputs", true);
        PropertiesUtils.set(properties, "useTwoPass", true);
        return properties;
    }
}
