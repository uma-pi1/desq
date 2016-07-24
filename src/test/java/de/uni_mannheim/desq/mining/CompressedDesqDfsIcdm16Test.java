package de.uni_mannheim.desq.mining;

import java.util.Properties;

import de.uni_mannheim.desq.util.PropertiesUtils;

public class CompressedDesqDfsIcdm16Test extends Icdm16TraditionalMiningTest {

	public CompressedDesqDfsIcdm16Test(long sigma, int gamma, int lambda, boolean generalize) {
		super(sigma, gamma, lambda, generalize);
	}

	@Override
	public Class<? extends DesqMiner> getMinerClass() {
		return CompressedDesqDfs.class;
	}

	@Override
	public Properties createProperties() {
		Properties properties = new Properties();
        PropertiesUtils.set(properties, "minSupport", sigma);
        PropertiesUtils.set(properties, "patternExpression", DesqMiner.patternExpressionFor(gamma, lambda, generalize));
        return properties;
	}

}
