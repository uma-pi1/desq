package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.util.DesqProperties;
import old.journal.mining.*;
import de.uni_mannheim.desq.mining.*;
import org.apache.commons.configuration2.ConfigurationConverter;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;


public class CompareDesqCount {

	// Input parameters 
	//String patternExpression = "([.^ . .]|[. .^ .]|[. . .^])";
	//String patternExpression = "(.^){0,3}";
	String patternExpression = "(.^ JJ@ NN@)";
	int sigma = 1000;
	
	
	// IO
	String inputFile = "data-local/nyt-1991-data.del";
	String dictFile = "data-local/nyt-1991-dict.avro.gz";
	String outputFile = "tmp/output";


	public void runOld(boolean iterative, boolean pruneIrrelevantInputs, boolean useTwoPass) throws IOException {
		Dictionary dict = Dictionary.loadFrom(dictFile);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(inputFile), true);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		DelPatternWriter patternWriter = new DelPatternWriter(
				new FileOutputStream(outputFile+"-OldDesqCount-"+iterative+"-"+pruneIrrelevantInputs+"-"+useTwoPass),
				DelPatternWriter.TYPE.SID);
		patternWriter.setDictionary(dict);
		ctx.patternWriter = patternWriter;
		Properties properties;
		if (iterative) {
			if (useTwoPass) {
				properties = DesqCountIterativeTwoPass.createProperties(patternExpression, sigma);
			} else if (pruneIrrelevantInputs) {
				properties = DesqCountIterativeWithPruning.createProperties(patternExpression, sigma);
			} else {
				properties = ConfigurationConverter.getProperties(DesqCountIterative.createConf(patternExpression, sigma));
			}
		} else {
			if (useTwoPass) {
				properties = DesqCountTwoPass.createProperties(patternExpression, sigma);
			} else if (pruneIrrelevantInputs) {
				properties = DesqCountWithPruning.createProperties(patternExpression, sigma);
			} else {
				throw new UnsupportedOperationException();
			}
		}
		ctx.conf = new DesqProperties(properties);
		ExampleUtils.runMiner(dataReader, ctx);
		System.out.println();
		patternWriter.close();
	}

	public void run(boolean iterative, boolean pruneIrrelevantInputs, boolean useTwoPass) throws IOException {
		Dictionary dict = Dictionary.loadFrom(dictFile);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(inputFile), true);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		DelPatternWriter patternWriter = new DelPatternWriter(
		        new FileOutputStream(outputFile+"-DesqCount-"+iterative+"-"+pruneIrrelevantInputs+"-"+useTwoPass),
				DelPatternWriter.TYPE.SID);
		patternWriter.setDictionary(dict);
		ctx.patternWriter = patternWriter;
		ctx.conf = DesqCount.createConf(patternExpression, sigma);
		ctx.conf.setProperty("desq.mining.iterative", iterative);
		ctx.conf.setProperty("desq.mining.prune.irrelevant.inputs", pruneIrrelevantInputs);
		ctx.conf.setProperty("desq.mining.use.two.pass", useTwoPass);

		ExampleUtils.runMiner(dataReader, ctx);
		System.out.println();
		patternWriter.close();
	}
	
	public static void main(String[] args) throws Exception {
		CompareDesqCount cdc = new CompareDesqCount();

		System.out.println("OLD JOURNAL METHODS");
		System.out.println("-------------------");
		cdc.runOld(false, true, false);
		cdc.runOld(false, true, true);
		cdc.runOld(true, false, false);
		cdc.runOld(true, true, false);
		cdc.runOld(true, true, true);

		System.out.println();
		System.out.println("NEW METHODS");
		System.out.println("-----------");
		cdc.run(false, false, false);
		cdc.run(false, true, false);
		cdc.run(false, true, true);
		cdc.run(true, false, false);
		cdc.run(true, true, false);
		cdc.run(true, true, true);
	}
	
	
}
