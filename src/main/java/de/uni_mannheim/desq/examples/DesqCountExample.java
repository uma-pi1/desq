package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.mining.DesqCount;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;

public class DesqCountExample {
	void nyt() throws IOException {
		int sigma = 100000;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;
		String patternExp = DesqCount.patternExpressionFor(gamma, lambda, generalize);

		ExampleUtils.runNyt( DesqCount.createConf(patternExp, sigma) );
	}

	public static void icdm16() throws IOException {
		String patternExpression = "[c|d]([A^|B=^]+)e";
		int sigma = 2;

		Configuration conf = DesqCount.createConf(patternExpression, sigma);
		//conf.setProperty("desq.mining.iterative", false);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		ExampleUtils.runIcdm16(conf);
	}

	public static void main(String[] args) throws IOException {
		icdm16();
		//nyt();
	}
}
