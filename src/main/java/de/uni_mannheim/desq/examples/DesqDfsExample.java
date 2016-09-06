package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.mining.DesqDfs;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;

public class DesqDfsExample {
	public static void nyt() throws IOException {
		int sigma = 10;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;
		String patternExp = DesqDfs.patternExpressionFor(gamma, lambda, generalize);
		patternExp = "(JJ@ JJ@ NN@)";

		Configuration conf = DesqDfs.createConf(patternExp, sigma);
		// conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		ExampleUtils.runNyt(conf);
	}

	public static void icdm16() throws IOException {
		String patternExp= "[c|d]([A^|B=^]+)e";
		int sigma = 2;

		Configuration conf = DesqDfs.createConf(patternExp, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.two.pass", true);
		ExampleUtils.runIcdm16(conf);
	}

	public static void main(String[] args) throws IOException {
		icdm16();
		//nyt();
	}
}
