package old.journal.examples;

import de.uni_mannheim.desq.mining.WeightedSequence;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.net.URL;

import de.uni_mannheim.desq.dictionary.Dictionary;
import old.de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import old.journal.mining.DesqDfsTwoPass;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import org.apache.commons.configuration2.ConfigurationConverter;

public class DesqDfsTwoPassExample {
	void icdm16() throws IOException {

		URL dictFile = getClass().getResource("/icdm16-example/dict.del");
		URL dataFile = getClass().getResource("/icdm16-example/data.del");

		// load the dictionary
		Dictionary dict = DictionaryIO.loadFromDel(dictFile.openStream(), false);

		// update hierarchy
		SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dict.incCounts(dataReader);
		dict.recomputeFids();
		System.out.println("Dictionary with statitics");
		DictionaryIO.saveToDel(System.out, dict, true, true);

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
		int sigma = 2;

		dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		MemoryPatternWriter result = new MemoryPatternWriter();
		ctx.patternWriter = result;
		ctx.conf = ConfigurationConverter.getConfiguration(DesqDfsTwoPass.createProperties(patternExpression, sigma));
		System.out.println("\nPatterns " + ConfigurationConverter.getProperties(ctx.conf));
		
		DesqMiner miner = new DesqDfsTwoPass(ctx);
		miner.addInputSequences(dataReader);
		miner.mine();
		
		System.out.println("P-frequent sequences");
		for (WeightedSequence pattern : result.getPatterns()) {
			System.out.print(pattern.support);
			System.out.print(": ");
			System.out.println(dict.getItemsByFids(pattern.items));
		}
	}

	public static void main(String[] args) throws IOException {
		new DesqDfsTwoPassExample().icdm16();
	}
}