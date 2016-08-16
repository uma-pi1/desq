package de.uni_mannheim.desq.dfsold;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;


public class DfsDriver {
	
	private static final Stopwatch ioTime = Stopwatch.createUnstarted();
	private static final Stopwatch miningTime = Stopwatch.createUnstarted();
	

	/** <input> <dict> <output> <sigma> <gamma> <lambda> <0/1?> <0?> 
	 * @throws Exception */
	public static void run(String sequenceFile, String dictionaryFile, String outputFile, int sigma, int gamma, int lambda, boolean generalize) throws Exception {
		boolean writeOutput = true;
		
		/** load dictionary */
		Dictionary dict = Dictionary.getInstance();
		dict.load(dictionaryFile, sigma);
		
		
		/** initialize hierarchy */
		int[] itemToParent = dict.getItemToParent();
		SimpleHierarchy.getInstance().initialize(itemToParent);
		
		/** initialize writer */
		
		if(writeOutput) {
		SequentialWriter writer = SequentialWriter.getInstance();
			writer.setItemIdToItemMap(dict.getItemIdToName());
			writer.setOutputPath(outputFile);
		}
		
		Dfs dfs = new Dfs(sigma, gamma,lambda, generalize, writeOutput);
		
		ioTime.start();
		dfs.scanDatabase(sequenceFile);
		ioTime.stop();
		
		miningTime.start();
		dfs.mine();
		miningTime.stop();
		
		
		
		String s = null;
		s = "PrefixGrowth-OLD" 
		+ "\t" + sigma 
		+ "\t" + gamma 
		+ "\t" + lambda
		+ "\t" + generalize
		+ "\t" + ioTime.elapsed(TimeUnit.SECONDS)
		+ "\t" + miningTime.elapsed(TimeUnit.SECONDS);
		
		
		System.out.println(s);
	}

}
