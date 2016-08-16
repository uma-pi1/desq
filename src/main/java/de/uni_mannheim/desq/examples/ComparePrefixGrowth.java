package de.uni_mannheim.desq.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import de.uni_mannheim.desq.dfsold.DfsDriver;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;

import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.PrefixGrowthMiner;

public class ComparePrefixGrowth {

	int sigma;
	int gamma;
	int lambda;
	boolean generalize;
	String dataFile;
	String dictFile;
	String outFile;
	
	public ComparePrefixGrowth(int sigma, int gamma, int lambda, boolean generalize) {
		this.sigma = sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.generalize = generalize;
	}
	
	public void setIO(String dataFile, String dictFile, String outFile) {
		this.dataFile = dataFile;
		this.dictFile = dictFile;
		this.outFile = outFile;
	}
	
	public void oldPrefixGrowth() throws Exception {
		DfsDriver.run(dataFile, dictFile, outFile, sigma, gamma, lambda, generalize);
	}
	
	public void newPrefixGrowth() throws IOException {
		Stopwatch ioTime = Stopwatch.createUnstarted();
		Stopwatch miningTime = Stopwatch.createUnstarted();
		//Create output file
		File out = new File(outFile + "/new");
		File parentOut = out.getParentFile();
		//if(!parentOut.exists()) {
//			parentOut.mkdirs();
		//}
		out.delete();
		
		
		Dictionary dict = DictionaryIO.loadFromDel(new FileInputStream(dictFile), true);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), true);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dict;
		DelPatternWriter patternWriter = new DelPatternWriter(new FileOutputStream(out), true);
		patternWriter.setDictionary(dict);
		ctx.patternWriter = patternWriter;
		ctx.properties = PrefixGrowthMiner.createProperties(sigma, gamma, lambda, generalize);
		
		DesqMiner miner =  new PrefixGrowthMiner(ctx);
		
		
		ioTime.start();
		miner.addInputSequences(dataReader);
		ioTime.stop();
		
		miningTime.start();
		miner.mine();
		miningTime.stop();
		
		patternWriter.close();
		
		StringBuilder sb = new StringBuilder();
		sb.append("PrefixGrowth-NEW\t");
		sb.append(sigma + "\t");
		sb.append(gamma + "\t");
		sb.append(lambda + "\t");
		sb.append(generalize + "\t");
		sb.append(ioTime.elapsed(TimeUnit.SECONDS));
		sb.append("\t");
		sb.append(miningTime.elapsed(TimeUnit.SECONDS));
		
		System.out.println(sb.toString());
	
	}
	
	public static void main(String[] args) throws Exception {
		
		int sigma = 100000;
		int gamma = 0;
		int lambda = 2;
		boolean generalize = true;
		
		String dataFile = "data-local/nyt-1991-data.del";
		String dictFile = "data-local/nyt-1991-dict.del";
		String outFile = "./tmp";
		
		ComparePrefixGrowth cpg = new ComparePrefixGrowth(sigma, gamma, lambda, generalize);
		cpg.setIO(dataFile, dictFile, outFile);
		
		cpg.oldPrefixGrowth();
		cpg.newPrefixGrowth();
	}

}
