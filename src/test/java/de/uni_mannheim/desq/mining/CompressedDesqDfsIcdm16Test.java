package de.uni_mannheim.desq.mining;

import org.apache.commons.configuration2.Configuration;

public class CompressedDesqDfsIcdm16Test extends Icdm16TraditionalMiningTest {

	public CompressedDesqDfsIcdm16Test(long sigma, int gamma, int lambda, boolean generalize) {
		super(sigma, gamma, lambda, generalize);
	}

	@Override
	public Configuration createConf() {
		String patternExpression = DesqMiner.patternExpressionFor(gamma, lambda, generalize);
		Configuration conf = CompressedDesqDfs.createConf(patternExpression, sigma);
		return conf;
	}

}
