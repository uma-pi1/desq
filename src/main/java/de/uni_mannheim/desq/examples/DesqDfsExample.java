package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.mining.DesqDfs;

import java.io.IOException;

public class DesqDfsExample {
	void nyt() throws IOException {
		int sigma = 100000;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;
		String patternExp = DesqDfs.patternExpressionFor(gamma, lambda, generalize);

		ExampleUtils.runNyt( DesqDfs.createProperties(patternExp, sigma) );
	}

	public static void icdm16() throws IOException {
		String patternExpression = "[c|d]([A^|B=^]+)e";
		int sigma = 2;

		ExampleUtils.runIcdm16( DesqDfs.createProperties(patternExpression, sigma) );
	}

	public static void main(String[] args) throws IOException {
		icdm16();
		//nyt();
	}
}
