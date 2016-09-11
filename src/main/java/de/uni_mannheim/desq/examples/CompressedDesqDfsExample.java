package de.uni_mannheim.desq.examples;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.net.URL;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.CompressedDesqDfs;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.Pattern;

public class CompressedDesqDfsExample {

	void icdm16() throws IOException {

		URL dictFile = getClass().getResource("/icdm16-example/dict.json");
		URL dataFile = getClass().getResource("/icdm16-example/data.del");

		// load the dictionary
		Dictionary dict = Dictionary.loadFrom(dictFile);

		// update hierarchy
		SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dict.incCounts(dataReader);
		dict.recomputeFids();
		System.out.println("Dictionary with statitics");
		dict.writeJson(System.out);
		System.out.println();

		// print sequences
		System.out.println("Input sequences:");
		dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dataReader.setDictionary(dict);
		IntList inputSequence = new IntArrayList();
		while (dataReader.readAsFids(inputSequence)) {
			System.out.println(dict.getItemsByFids(inputSequence));
		}

		// input parameters
		String patternExpression = "[c|d]([A^|B=^]+)e";
		//String patternExpression = "(B^)";
		int sigma = 2;

		dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		MemoryPatternWriter result = new MemoryPatternWriter();
		ctx.patternWriter = result;
		ctx.conf = CompressedDesqDfs.createConf(patternExpression, sigma);
		System.out.println("\nPatterns " + ctx.conf.toString());
		
		DesqMiner miner = new CompressedDesqDfs(ctx);
		miner.addInputSequences(dataReader);
		miner.mine();
		
		System.out.println("P-frequent sequences");
		for (Pattern pattern : result.getPatterns()) {
			System.out.print(pattern.getFrequency());
			System.out.print(": ");
			System.out.println(dict.getItemsByFids(pattern.getItemFids()));
		}
	}

	public static void main(String[] args) throws IOException {
		new CompressedDesqDfsExample().icdm16();
	}
}
