package de.uni_mannheim.desq.examples;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.journal.mining.DesqCountIterative;
import de.uni_mannheim.desq.mining.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class CompareDesqCount {

	// Input parameters 
	//String patternExpression = "([.^ . .]|[. .^ .]|[. . .^])";
	//String patternExpression = "(.^){0,3}";
	String patternExpression = "(.^ JJ@ NN@)";
	int sigma = 1000;
	
	
	// IO
	String inputFile = "data-local/nyt-1991-data.del";
	String dictFile = "data-local/nyt-1991-dict.del";
	String outputFile = "tmp/output";
	
	
	public void run(boolean iterative) throws IOException {
		Dictionary dict = DictionaryIO.loadFromDel(new FileInputStream(dictFile), true);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(inputFile), true);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		DelPatternWriter patternWriter = new DelPatternWriter(
		        new FileOutputStream(outputFile+"-DesqCount-"+iterative), DelPatternWriter.TYPE.SID);
		patternWriter.setDictionary(dict);
		ctx.patternWriter = patternWriter;
		ctx.conf = DesqCount.createConf(patternExpression, sigma);
		ctx.conf.setProperty("desq.mining.iterative", iterative);

		ExampleUtils.runMiner(dataReader, ctx);
		patternWriter.close();
		
	}
	
	public static void main(String[] args) throws Exception {
		CompareDesqCount cdc = new CompareDesqCount();

        /*
		cdd.desqDfsOld();
		System.out.println("AutomatonTime = " + automatonTime.elapsed(TimeUnit.SECONDS));
		System.out.println("IOTime = " + ioTime.elapsed(TimeUnit.SECONDS));
		System.out.println("MiningTime = " + miningTime.elapsed(TimeUnit.SECONDS));
		*/

		cdc.run(false);
		cdc.run(false);
		cdc.run(true);
		cdc.run(true);
	}
	
	
}
