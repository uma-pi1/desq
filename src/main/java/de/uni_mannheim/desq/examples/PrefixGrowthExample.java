package de.uni_mannheim.desq.examples;

import java.io.IOException;

import de.uni_mannheim.desq.mining.*;

public class PrefixGrowthExample {
	public static void nyt() throws IOException {
		int sigma = 10000;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;

		ExampleUtils.runNyt( PrefixGrowthMiner.createConf(sigma,gamma,lambda,generalize) );
	}

	public static void icdm16() throws IOException {
		int sigma = 3;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;

		ExampleUtils.runIcdm16( PrefixGrowthMiner.createConf(sigma,gamma,lambda,generalize) );
	}

	public static void main(String[] args) throws IOException {
		// nyt();
		icdm16();
	}
}
