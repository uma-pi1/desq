package driver;

import fst.XFst;
import fst.Fst;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Stopwatch;

import driver.DesqConfig.Match;
import patex.PatEx;
import mining.DesqCount;
import mining.OnePassIterative;
//import mining.OnePassIterative;
import mining.OnePassRecursive;
import mining.interestingness.DesqCountScored;
import mining.interestingness.OnePassIterativeScored;
//import mining.TwoPass;
import utils.Dictionary;
//import writer.LogWriter;
import writer.SequentialWriter;

/**
 * DesqCountDriver.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class DesqCountScoredDriver {
	
	// Timers
	public static Stopwatch fstTime = Stopwatch.createUnstarted();
	public static Stopwatch totalTime = Stopwatch.createUnstarted();
	
	private static final Logger logger = Logger.getLogger(DesqCountScoredDriver.class.getSimpleName());
	
	public static void run(DesqConfig conf) throws Exception {
		
		
		String input = conf.getEncodedSequencesPath();
		String output = conf.getOutputSequencesPath();
		String patternExpression = conf.getPatternExpression();
		Match match = conf.getMatch();
		if(match == Match.PARTIAL || match == Match.RSTRICT) {
			patternExpression = ".* [" + patternExpression.trim() + "]";
		}
		double support = conf.getSigma();
		
		boolean writeOutput = conf.isWriteOutput();
		boolean useFlist = conf.isUseFlist();
		
		
		String sequenceFile = input.concat("/raw/part-r-00000");
		String dictionaryFile = input.concat("/wc/part-r-00000");
		
		
		/** load dictionary */
		Dictionary dict = Dictionary.getInstance();
		dict.load(dictionaryFile);
		
		/** initialize writer */
		if(writeOutput) {
		SequentialWriter writer = SequentialWriter.getInstance();
			writer.setItemIdToItemMap(dict.getItemIdToName());
			writer.setOutputPath(output);
		}	
			
		
		logger.log(Level.INFO, "Parsing pattern expression and generating FST");
		fstTime.start();
		
		PatEx ex = new PatEx(patternExpression);
		// Generate cFST
		Fst cFst = ex.translateToFst();
		cFst.minimize();
	
		// Generate optimized cFst
		XFst xFst = cFst.optimizeForExecution();
		
		logger.log(Level.INFO, "Took "+ fstTime.elapsed(TimeUnit.MILLISECONDS) + "ms");
		
		logger.log(Level.INFO, "Mining P-frequent sequences...");
		
		//DesqCount dc = new OnePassRecursive(support, xFst, writeOutput, useFlist);
		DesqCountScored dc = new OnePassIterativeScored(support, xFst, writeOutput, useFlist, match);
		
		totalTime.start();
		
		dc.scan(sequenceFile);
		
		totalTime.stop();
		
		logger.log(Level.INFO, "Took " + totalTime.elapsed(TimeUnit.SECONDS) +"s");
	
	}

}
