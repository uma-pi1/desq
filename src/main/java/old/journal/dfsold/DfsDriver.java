package old.journal.dfsold;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;


public class DfsDriver {
	
	private static final Stopwatch ioTime = Stopwatch.createUnstarted();
	private static final Stopwatch miningTime = Stopwatch.createUnstarted();
	

	/** <input> <dict> <output> <sigma> <gamma> <lambda> <0/1?> <0?> 
	 * @throws Exception */
	public static void run(String sequenceFile, String dictionaryFile, String outputFile, int sigma, int gamma, int lambda, boolean generalize) throws Exception {
		boolean writeOutput = false;
		
		/** load dictionary */
		Dictionary dict = Dictionary.getInstance();
		dict.load(dictionaryFile, sigma);
		
		
		/** initialize hierarchy */
		int[] itemToParent = dict.getItemToParent();
		SimpleHierarchy.getInstance().initialize(itemToParent);
		
		/** initialize old.writer */
		
		if(writeOutput) {
		SequentialWriter writer = SequentialWriter.getInstance();
			writer.setItemIdToItemMap(dict.getItemIdToName());
			writer.setOutputPath(outputFile);
		}
		
		Dfs dfs = new Dfs(sigma, gamma,lambda, generalize, writeOutput);
		dfs.flist = dict.fList;

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
		+ "\t" + dfs.noOutputPatterns()
		+ "\t" + ioTime.elapsed(TimeUnit.MILLISECONDS)
		+ "\t" + miningTime.elapsed(TimeUnit.MILLISECONDS)
		+ "\t" + (ioTime.elapsed(TimeUnit.MILLISECONDS) + miningTime.elapsed(TimeUnit.MILLISECONDS));


		System.out.println(s);
	}

}
