package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.mining.DesqCount;

import java.io.IOException;

public class DesqCountExample {
	void nyt() throws IOException {
		int sigma = 100000;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;
		String patternExp = DesqCount.patternExpressionFor(gamma, lambda, generalize);

		ExampleUtils.runNyt( DesqCount.createProperties(patternExp, sigma) );
	}

	public static void icdm16() throws IOException {
		String patternExpression = "[c|d]([A^|B=^]+)e";
		int sigma = 2;

		ExampleUtils.runIcdm16( DesqCount.createProperties(patternExpression, sigma) );
	}

	public static void main(String[] args) throws IOException {
		icdm16();
		//nyt();
	}
}
