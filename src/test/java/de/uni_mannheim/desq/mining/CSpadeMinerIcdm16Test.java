package de.uni_mannheim.desq.mining;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Properties;

/**
 * Created by rgemulla on 16.7.2016.
 */
@RunWith(Parameterized.class)
public class CSpadeMinerIcdm16Test extends Icdm16TraditionalMiningTest {
    public CSpadeMinerIcdm16Test(long sigma, int gamma, int lambda, boolean generalize) {
        super(sigma, gamma, lambda, generalize);
    }

    @Override
    public Class<? extends DesqMiner> getMinerClass() {
        return CSpadeMiner.class;
    }

    @Override
    public Properties createProperties() {
        return CSpadeMiner.createProperties(sigma, gamma, lambda, generalize);
    }
}
