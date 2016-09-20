package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.util.DesqProperties;
import old.journal.mining.DesqDfsTwoPass;
import old.journal.mining.DesqDfsWithPruning;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;


public class CompareDesqDfs {

	// Input parameters 
	//String patternExpression = "([.^ . .]|[. .^ .]|[. . .^])";
	//String patternExpression = "(.^){0,3}";
	String patternExpression = "(.^ JJ@ NN@)";
	int sigma = 1000;
	
	
	// IO
	String inputFile = "data-local/nyt-1991-data.del";
	String dictFile = "data-local/nyt-1991-dict.avro.gz";
	String outputFile = "tmp/output";


	public void runOld(boolean pruneIrrelevantInputs, boolean useTwoPass) throws IOException {
		Dictionary dict = Dictionary.loadFrom(dictFile);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(inputFile), true);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		DelPatternWriter patternWriter = new DelPatternWriter(
				new FileOutputStream(outputFile+"-OldDesqDfs-"+pruneIrrelevantInputs+"-"+useTwoPass),
				DelPatternWriter.TYPE.SID);
		patternWriter.setDictionary(dict);
		ctx.patternWriter = patternWriter;
		Properties properties;
		if (useTwoPass) {
			properties = DesqDfsTwoPass.createProperties(patternExpression, sigma);
		} else if (pruneIrrelevantInputs) {
			properties = DesqDfsWithPruning.createProperties(patternExpression, sigma);
		} else {
			throw new UnsupportedOperationException();
		}
		ctx.conf = new DesqProperties(properties);
		ExampleUtils.runMiner(dataReader, ctx);
		System.out.println();
		patternWriter.close();
	}

	public void run(boolean pruneIrrelevantInputs, boolean useTwoPass) throws IOException {
		Dictionary dict = Dictionary.loadFrom(dictFile);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(inputFile), true);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		DelPatternWriter patternWriter = new DelPatternWriter(
		        new FileOutputStream(outputFile+"-DesqDfs-"+pruneIrrelevantInputs+"-"+useTwoPass),
				DelPatternWriter.TYPE.SID);
		patternWriter.setDictionary(dict);
		ctx.patternWriter = patternWriter;
		ctx.conf = DesqDfs.createConf(patternExpression, sigma);
		ctx.conf.setProperty("desq.mining.prune.irrelevant.inputs", pruneIrrelevantInputs);
		ctx.conf.setProperty("desq.mining.use.two.pass", useTwoPass);

		ExampleUtils.runMiner(dataReader, ctx);
		System.out.println();
		patternWriter.close();
	}
	
	public static void main(String[] args) throws Exception {
		CompareDesqDfs cdd = new CompareDesqDfs();

		System.out.println("OLD JOURNAL METHODS");
		System.out.println("-------------------");
		cdd.runOld(true, false);
		cdd.runOld(true, true);

		System.out.println();
		System.out.println("NEW METHODS");
		System.out.println("-----------");
		cdd.run(false, false);
		cdd.run(true, false);
		cdd.run(true, true);
	}
	
	
}
