package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.util.PropertiesUtils;

import java.io.IOException;
import java.util.Properties;

public class DesqDfsExample {
	public static void nyt() throws IOException {
		int sigma = 1000;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;
		String patternExp = DesqDfs.patternExpressionFor(gamma, lambda, generalize);

		Properties properties = DesqDfs.createProperties(patternExp, sigma);
		ExampleUtils.runNyt(properties);
	}

	public static void icdm16() throws IOException {
		String patternExp= "[c|d]([A^|B=^]+)e";
		int sigma = 2;

		Properties properties = DesqDfs.createProperties(patternExp, sigma);
		PropertiesUtils.set(properties, "pruneIrrelevantInputs", true);
		ExampleUtils.runIcdm16(properties);
	}

	public static void main(String[] args) throws IOException {
		icdm16();
		//nyt();
	}
}
