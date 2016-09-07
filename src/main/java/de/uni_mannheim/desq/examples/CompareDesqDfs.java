package de.uni_mannheim.desq.examples;

import java.io.FileInputStream;
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
	//String patternExpression = "(.^){0,3}";
	String patternExpression = "(.^ JJ@ NN@)";
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
		
		old.utils.Dictionary dict = old.utils.Dictionary.getInstance();
		dict.load(dictFile);
		
		if (writeOutput) {
			old.writer.DelWriter wr = old.writer.DelWriter.getInstance();
			wr.setItemIdToItemMap(dict.getItemIdToName());
			wr.setOutputPath(outputFile);
		}
	
	
		System.out.println("Translating pattern expr. " + patternExpression);
		
		automatonTime.start();
		String pe = ".*[" + patternExpression + "]";
		old.patex.PatExOld ex = new old.patex.PatExOld(pe);
		// Generate cFST
		old.fst.Fst cFst = ex.translateToFst();
		cFst.minimize();
		// Generate optimized cFst
		old.fst.XFst xFst = cFst.optimizeForExecution();
		automatonTime.stop();
		
		old.mining.DesqDfs dd = new old.mining.DfsOnePass(sigma, xFst, writeOutput);
		
		
		System.out.println("Reading input sequences...");
		ioTime.start();
		dd.scan(inputFile);
		ioTime.stop();
		
		
		System.out.println("Mining...");
		miningTime.start();
		dd.mine();
		miningTime.stop();
	}
	
	public void desqDfsNew(boolean pruneIrrelevantInputSequences, boolean useTwoPass) throws IOException {
		
		resetTimers();
		
		Dictionary dict = DictionaryIO.loadFromDel(new FileInputStream(dictFile), true);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(inputFile), true);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		DelPatternWriter patternWriter = new DelPatternWriter(
		        new FileOutputStream(outputFile+"-NEW-"+pruneIrrelevantInputSequences+"-"+useTwoPass), DelPatternWriter.TYPE.SID);
		patternWriter.setDictionary(dict);
		ctx.patternWriter = patternWriter;
		ctx.conf = DesqDfs.createConf(patternExpression, sigma);
		ctx.conf.setProperty("desq.mining.prune.irrelevant.inputs", pruneIrrelevantInputSequences);
		ctx.conf.setProperty("desq.mining.use.two.pass", useTwoPass);

		System.out.println("Translating pattern expr." + patternExpression);
		
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

        /*
		cdd.desqDfsOld();
		System.out.println("AutomatonTime = " + automatonTime.elapsed(TimeUnit.SECONDS));
		System.out.println("IOTime = " + ioTime.elapsed(TimeUnit.SECONDS));
		System.out.println("MiningTime = " + miningTime.elapsed(TimeUnit.SECONDS));
		*/

		cdd.desqDfsNew(false, false);
		System.out.println("AutomatonTime = " + automatonTime.elapsed(TimeUnit.SECONDS));
		System.out.println("IOTime = " + ioTime.elapsed(TimeUnit.SECONDS));
		System.out.println("MiningTime = " + miningTime.elapsed(TimeUnit.SECONDS));

		cdd.desqDfsNew(true, false);
		System.out.println("AutomatonTime = " + automatonTime.elapsed(TimeUnit.SECONDS));
		System.out.println("IOTime = " + ioTime.elapsed(TimeUnit.SECONDS));
		System.out.println("MiningTime = " + miningTime.elapsed(TimeUnit.SECONDS));

        cdd.desqDfsNew(true, true);
        System.out.println("AutomatonTime = " + automatonTime.elapsed(TimeUnit.SECONDS));
        System.out.println("IOTime = " + ioTime.elapsed(TimeUnit.SECONDS));
        System.out.println("MiningTime = " + miningTime.elapsed(TimeUnit.SECONDS));
	}
	
	
}
