package de.uni_mannheim.desq.examples;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;





public class CompareDesqDfs {

	// Input parameters 
	//String patternExpression = "([.^ . .]|[. .^ .]|[. . .^])";
	//String patternExpression = "(.^){3} NN@";
	String patternExpression = "NN@ .";
	int sigma = 1000;
	
	
	// IO
	String inputFile = "data-local/nyt-1991-data.del";
	String dictFile = "data-local/nyt-1991-dict.del";
	String outputFile = "tmp/output";
	
	
	// Timers
	private static final Stopwatch automatonTime = Stopwatch.createUnstarted();
	private static final Stopwatch ioTime = Stopwatch.createUnstarted();
	private static final Stopwatch miningTime = Stopwatch.createUnstarted();
	
	public static void resetTimers() {
		automatonTime.reset();
		ioTime.reset();
		miningTime.reset();
	}
	
	public void desqDfsOld() throws Exception {
		
		resetTimers();

		boolean writeOutput = true;
		
		utils.Dictionary dict = utils.Dictionary.getInstance();
		dict.load(dictFile);
		
		if (writeOutput) {
			writer.DelWriter wr = writer.DelWriter.getInstance();
			wr.setItemIdToItemMap(dict.getItemIdToName());
			wr.setOutputPath(outputFile);
		}
	
	
		System.out.println("Translating pattern expr.");
		
		automatonTime.start();
		String pe = ".*[" + patternExpression + "]";
		patex.PatExOld ex = new patex.PatExOld(pe);
		// Generate cFST
		fst.Fst cFst = ex.translateToFst();
		cFst.minimize();
		// Generate optimized cFst
		fst.XFst xFst = cFst.optimizeForExecution();
		automatonTime.stop();
		
		mining.DesqDfs dd = new mining.DfsOnePass(sigma, xFst, writeOutput);
		
		
		System.out.println("Reading input sequences...");
		ioTime.start();
		dd.scan(inputFile);
		ioTime.stop();
		
		
		System.out.println("Mining...");
		miningTime.start();
		dd.mine();
		miningTime.stop();
	}
	
	public void desqDfsNew() throws IOException {
		
		resetTimers();
		
		Dictionary dict = DictionaryIO.loadFromDel(new FileInputStream(dictFile), true);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(inputFile), true);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		DelPatternWriter patternWriter = new DelPatternWriter(new FileOutputStream(outputFile), true);
		patternWriter.setDictionary(dict);
		ctx.patternWriter = patternWriter;
		ctx.properties = DesqDfs.createProperties(patternExpression, sigma);
		
		System.out.println("Translating pattern expr.");
		
		automatonTime.start();
		DesqMiner miner = new DesqDfs(ctx);
		automatonTime.stop();
		
		
		System.out.println("Reading input sequences...");
		ioTime.start();
		miner.addInputSequences(dataReader);
		ioTime.stop();
		
		System.out.println("Mining...");
		miningTime.start();
		miner.mine();
		miningTime.stop();
		
		patternWriter.close();
		
	}
	
	public static void main(String[] args) throws Exception {
		CompareDesqDfs cdd = new CompareDesqDfs();
		
		
		cdd.desqDfsOld();
		
		System.out.println("AutomatonTime = " + automatonTime.elapsed(TimeUnit.SECONDS));
		System.out.println("IOTime = " + ioTime.elapsed(TimeUnit.SECONDS));
		System.out.println("MiningTime = " + miningTime.elapsed(TimeUnit.SECONDS));
		
		cdd.desqDfsNew();
		
		System.out.println("AutomatonTime = " + automatonTime.elapsed(TimeUnit.SECONDS));
		System.out.println("IOTime = " + ioTime.elapsed(TimeUnit.SECONDS));
		System.out.println("MiningTime = " + miningTime.elapsed(TimeUnit.SECONDS));
	}
	
	
}
