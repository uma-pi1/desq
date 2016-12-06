package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.mining.DesqCount;
import de.uni_mannheim.desq.util.DesqProperties;

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
		String patternExpression = "[[c|d]([A^|B=^]+)e]";
		//String patternExpression = "(A^) .* (A^) .* (A)";
		patternExpression = "(a1)..";
		int sigma = 2;

		DesqProperties conf = DesqCount.createConf(patternExpression, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.two.pass", false);
		ExampleUtils.runIcdm16(conf);
	}

	public static void main(String[] args) throws IOException {
		icdm16();
		//nyt();
	}
}
