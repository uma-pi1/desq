package driver;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import mining.interestingness.DesqCountScored;
import mining.interestingness.OnePassIterativeScored;
import mining.interestingness.OnePassRecursiveScored;
import mining.scores.DesqCountScore;
import mining.scores.FrequencyScore;
import mining.scores.RankedScoreList;
import mining.scores.RankedScoreListAll;
import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.scores.InformationGainScoreDesqCount;
import mining.scores.InformationGainScoreDesqCountMinSup;
import mining.scores.InformationGainScoreDesqCountNoPruning;
import patex.PatEx;
//import mining.TwoPass;
import utils.Dictionary;
import visual.GraphViz;
//import writer.LogWriter;
import writer.SequentialWriter;

import com.google.common.base.Stopwatch;

import driver.DesqConfig.Match;
import fst.Fst;
import fst.XFst;
//import mining.OnePassIterative;

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
		
		GraphViz.setTEMP_DIR("C:/Users/D053441/AppData/Local/Temp");
		GraphViz.setDOT("c:/Program Files (x86)/Graphviz2.38/bin/dot.exe");
		xFst.print("C:\\Users\\D053441\\Desktop\\output.pdf");
		
		
		logger.log(Level.INFO, "Took "+ fstTime.elapsed(TimeUnit.MILLISECONDS) + "ms");
		
		logger.log(Level.INFO, "Mining P-frequent sequences...");
		
//		GlobalInformationGainStatistic globalInformationGainStatistic = new GlobalInformationGainStatistic(sequenceFile);
//		DesqDfsScore score = new InformationGainScore(xFst.convertToFstGraph(), globalInformationGainStatistic, Dictionary.getInstance(), xFst);
		RankedScoreList rankedScoreList = new RankedScoreListAll(true);
		
		
//		DesqCountScore score = new FrequencyScore(xFst);
		DesqCountScore score = new FrequencyScore(xFst);
		HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?,?>, ?>> globalDataCollectors = score.getGlobalDataCollectors();
		
		
		//DesqCount dc = new OnePassRecursive(support, xFst, writeOutput, useFlist);
//		DesqCountScored dc = new OnePassIterativeScored(support, 
//															xFst, 
//															score, 
//															globalDataCollectors, rankedScoreList, writeOutput, match);
		
		DesqCountScored dc = new OnePassRecursiveScored(support, 
				xFst, 
				score, 
				globalDataCollectors, rankedScoreList, writeOutput, match);
		
		totalTime.start();
		
		dc.scan(sequenceFile);
		
		rankedScoreList.printList();
		
		totalTime.stop();
		
		logger.log(Level.INFO, "Took " + totalTime.elapsed(TimeUnit.SECONDS) +"s");
	
	}

}
