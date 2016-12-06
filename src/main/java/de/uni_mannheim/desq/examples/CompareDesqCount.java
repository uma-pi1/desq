package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqCount;
import de.uni_mannheim.desq.mining.DesqMinerContext;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class CompareDesqCount {

	// Input parameters 
	//String patternExpression = "([.^ . .]|[. .^ .]|[. . .^])";
	//String patternExpression = "(.^){0,3}";
	String patternExpression = "(.^ JJ@ NN@)";
	int sigma = 1000;
	//String patternExpression = "(ENTITY@^ VB@+ NN@+? IN@? ENTITY@^)";
	//int sigma = 100;
	
	
	// IO
	String inputFile = "data-local/nyt-1991-data.del";
	String dictFile = "data-local/nyt-1991-dict.avro.gz";
	String outputFile = "tmp/output";


	public void run(boolean pruneIrrelevantInputs, boolean useLazyDfa, boolean useTwoPass) throws IOException {
		Dictionary dict = Dictionary.loadFrom(dictFile);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(inputFile), true);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		DelPatternWriter patternWriter = new DelPatternWriter(
		        new FileOutputStream(outputFile+"-DesqCount-"+pruneIrrelevantInputs+"-"+useLazyDfa+"-"+useTwoPass),
				DelPatternWriter.TYPE.SID);
		patternWriter.setDictionary(dict);
		ctx.patternWriter = patternWriter;
		ctx.conf = DesqCount.createConf(patternExpression, sigma);
		ctx.conf.setProperty("desq.mining.prune.irrelevant.inputs", pruneIrrelevantInputs);
		ctx.conf.setProperty("desq.mining.use.lazy.dfa", useLazyDfa);
		ctx.conf.setProperty("desq.mining.use.two.pass", useTwoPass);

		ExampleUtils.runMiner(dataReader, ctx);
		System.out.println();
		patternWriter.close();
	}
	
	public static void main(String[] args) throws Exception {
		CompareDesqCount cdc = new CompareDesqCount();

		//cdc.run(false, false, false);
		cdc.run(true, false,false);
		cdc.run(true, true,false);
		cdc.run(true, false,true);
		cdc.run(true, true,true);
	}
}
