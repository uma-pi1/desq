package sandbox;

import java.io.FileInputStream;
import java.io.IOException;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.DfsMiner;

public class DfsMiningExample {
	static void icdm16() throws IOException {
		// load the dictionary
		Dictionary dict = DictionaryIO.loadFromDel(
				new FileInputStream("data/icdm16/example-dict.del"), false);
		
		// update hierarchy
		SequenceReader dataReader = new DelSequenceReader(
				new FileInputStream("data/icdm16/example-data.del"), false);
		dict.incCounts(dataReader);
		dict.recomputeFids();
		DictionaryIO.saveToDel(System.out, dict, true, true);

		// Perform mining 
		dataReader = new DelSequenceReader(
				new FileInputStream("data/icdm16/example-data.del"), false);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.sigma = 4;
		MemoryPatternWriter result = new MemoryPatternWriter();
		ctx.patternWriter = result;
		ctx.dict = dict;
		DesqMiner miner = new DfsMiner(ctx,0,100,true);
		miner.addInputSequences(dataReader);
		miner.mine();
		
		for (int i=0; i<result.size(); i++) {
			System.out.print(result.getFrequency(i));
			System.out.print(": ");
			System.out.println(dict.getItemsByFids(result.getPattern(i)));
		}
	}

	public static void main(String[] args) throws IOException {
		//nyt();
		icdm16();
	}

}
