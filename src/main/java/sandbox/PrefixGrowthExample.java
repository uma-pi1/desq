package sandbox;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.PrefixGrowthMiner;
import de.uni_mannheim.desq.mining.Pattern;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class PrefixGrowthExample {
	void nyt() throws IOException {
		Dictionary dict = DictionaryIO.loadFromDel(new FileInputStream("data-local/nyt-1991-dict.del"), true);
		File dataFile = new File("data-local/nyt-1991-data.del");

		// Perform mining
		int sigma = 10000;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;

		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), true);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		MemoryPatternWriter result = new MemoryPatternWriter();
		ctx.patternWriter = result;
		ctx.properties = PrefixGrowthMiner.createProperties(sigma,gamma,lambda,generalize);
		System.out.println("\nPatterns " + ctx.properties.toString());
		DesqMiner miner = new PrefixGrowthMiner(ctx);
		miner.addInputSequences(dataReader);
		miner.mine();

		for (Pattern pattern : result.getPatterns()) {
			System.out.print(pattern.getFrequency());
			System.out.print(": ");
			System.out.println(dict.getItemsByFids(pattern.getItemFids()));
		}
	}

	void icdm16() throws IOException {
		URL dictFile = getClass().getResource("/icdm16-example/dict.del");
		URL dataFile = getClass().getResource("/icdm16-example/data.del");

		// load the dictionary
		Dictionary dict = DictionaryIO.loadFromDel(dictFile.openStream(), false);

		// update hierarchy
		SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dict.incCounts(dataReader);
		dict.recomputeFids();
		//DictionaryIO.saveToDel(System.out, dict, true, true);

		// print sequences
		System.out.println("Input sequences:");
		dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dataReader.setDictionary(dict);
		IntList inputSequence = new IntArrayList();
		while (dataReader.readAsFids(inputSequence)) {
			System.out.println(dict.getItemsByFids(inputSequence));
		}
		
		// Perform mining 
		int sigma = 3;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;
		
		dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
        ctx.dict = dict;
		MemoryPatternWriter result = new MemoryPatternWriter();
		ctx.patternWriter = result;
        ctx.properties = PrefixGrowthMiner.createProperties(sigma,gamma,lambda,generalize);
		System.out.println("\nPatterns " + ctx.properties.toString());
		DesqMiner miner = new PrefixGrowthMiner(ctx);
		miner.addInputSequences(dataReader);
		miner.mine();
		
		for (Pattern pattern : result.getPatterns()) {
			System.out.print(pattern.getFrequency());
			System.out.print(": ");
			System.out.println(dict.getItemsByFids(pattern.getItemFids()));
		}
	}

	public static void main(String[] args) throws IOException {
		//new PrefixGrowthExample().nyt();
		new PrefixGrowthExample().icdm16();
	}
}
