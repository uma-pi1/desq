package de.uni_mannheim.desq.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class PrefixGrowthExample {
	public static void nyt() throws IOException {
		int sigma = 10000;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;

		ExampleUtils.runNyt( PrefixGrowthMiner.createProperties(sigma,gamma,lambda,generalize) );
	}

	public static void icdm16() throws IOException {
		int sigma = 3;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;

		ExampleUtils.runIcdm16( PrefixGrowthMiner.createProperties(sigma,gamma,lambda,generalize) );
	}

	public static void main(String[] args) throws IOException {
		// nyt();
		icdm16();
	}
}
