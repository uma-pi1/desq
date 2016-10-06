package old.driver;

import com.google.common.base.Stopwatch;
import old.driver.DesqConfig.Match;
import old.fst.Fst;
import old.fst.XFst;
import old.mining.DesqCount;
import old.mining.OnePassIterative;
import old.patex.PatExOld;
import old.utils.Dictionary;
import old.writer.DelWriter;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

//import old.mining.OnePassIterative;
//import old.mining.TwoPass;

/**
 * DesqCountDriver.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class DesqCountDriver {
	
	// Timers
	public static Stopwatch fstTime = Stopwatch.createUnstarted();
	public static Stopwatch totalTime = Stopwatch.createUnstarted();
	
	private static final Logger logger = Logger.getLogger(DesqCountDriver.class.getSimpleName());
	
	public static void run(DesqConfig conf) throws Exception {
		
		
		String input = conf.getEncodedSequencesPath();
		String output = conf.getOutputSequencesPath();
		String patternExpression = conf.getPatternExpression();
		Match match = conf.getMatch();
		if(match == Match.PARTIAL || match == Match.RSTRICT) {
			patternExpression = ".* [" + patternExpression.trim() + "]";
		}
		int support = conf.getSigma();
		
		boolean writeOutput = conf.isWriteOutput();
		boolean useFlist = conf.isUseFlist();
		
		
		String sequenceFile = input.concat("/raw/part-r-00000");
		String dictionaryFile = input.concat("/wc/part-r-00000");
		
		
		/** load dictionary */
		Dictionary dict = Dictionary.getInstance();
		dict.load(dictionaryFile);
		
		/** initialize old.writer */
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
		
		//DesqCount dc = new OnePassRecursive(support, xFst, writeOutput, useFlist);
		DesqCount dc = new OnePassIterative(support, xFst, writeOutput, useFlist, match);
		
		totalTime.start();
		
		dc.scan(sequenceFile);
		
		totalTime.stop();
		
		logger.log(Level.INFO, "Took " + totalTime.elapsed(TimeUnit.SECONDS) +"s");
	
	}

}
