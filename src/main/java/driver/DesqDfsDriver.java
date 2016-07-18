package driver;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Stopwatch;

import fst.Fst;
import fst.XFst;
import mining.DesqDfs;
import mining.DfsOnePass;
import patex.PatExOld;
import utils.Dictionary;
import writer.DelWriter;

/**
 * DesqDfsDriver.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class DesqDfsDriver {

	// Timers
	public static Stopwatch totalTime = Stopwatch.createUnstarted();
	public static Stopwatch fstTime = Stopwatch.createUnstarted();
	
	private static Logger logger = Logger.getLogger(DesqDfsDriver.class.getSimpleName());
	
	public static void run(DesqConfig conf) throws Exception {
		String input = conf.getEncodedSequencesPath();
		String output = conf.getOutputSequencesPath();
		String patternExpression = conf.getPatternExpression();
		patternExpression = ".*[" + patternExpression.trim() + "]";
		int support = conf.getSigma();
		
		boolean writeOutput = conf.isWriteOutput();
		boolean useFlist = conf.isUseFlist();
	
		
		String sequenceFile = input.concat("/raw/part-r-00000");
		String dictionaryFile = input.concat("/wc/part-r-00000");
		
		
		/** load dictionary */
		Dictionary dict = Dictionary.getInstance();
		dict.load(dictionaryFile);
		
		/** initialize writer */
		if(writeOutput) {
		DelWriter writer = DelWriter.getInstance();
			writer.setItemIdToItemMap(dict.getItemIdToName());
			writer.setOutputPath(output);
		}	
			
		
		logger.log(Level.INFO, "Parsing pattern expression and generating FST");
		fstTime.start();
		
		PatExOld ex = new PatExOld(patternExpression);
		
		// Generate cFST
		Fst cFst = ex.translateToFst();
		cFst.minimize();
		
				
		// Generate optimized cFst
		XFst xFst = cFst.optimizeForExecution();
		
		logger.log(Level.INFO, "Took "+ fstTime.elapsed(TimeUnit.MILLISECONDS) + "ms");
		
		logger.log(Level.INFO, "Mining P-frequent sequences...");
		
		DesqDfs dd = new DfsOnePass(support, xFst, writeOutput);
		
		
		totalTime.start();
		
		dd.scan(sequenceFile);
		dd.mine();

		totalTime.stop();

		logger.log(Level.INFO, "Took " + totalTime.elapsed(TimeUnit.SECONDS) +"s");
	}

}
