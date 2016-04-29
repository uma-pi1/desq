package driver;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import mining.DesqDfs;
import mining.DfsOnePass;
import patex.PatEx;
import utils.Dictionary;
import writer.LogWriter;
import writer.SequentialWriter;
import fst.Fst;
import fst.XFst;

/**
 * DesqDfsDriver.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class DesqDfsDriver {

	// Timers
	public static Stopwatch totalTime = Stopwatch.createUnstarted();
	
	/** <input> <output> <pattern> <support> <logfile> <writeOutput:0/1> <useflist: 0/1> <method id: 0/1>
	 * @throws Exception */
	public static void main(String[] args) throws Exception {
		System.out.println("DESQ-DFS : " + Arrays.toString(args));
		String input = args[0];
		String output = args[1];
		String patternExpression = args[2];
		patternExpression = ".*[" + patternExpression.trim() + "]";
		int support = Integer.parseInt(args[3]);
		String logfile = args[4];
		boolean writeOutput = (args[5].equals("0")) ? false : true;
		boolean useFlist = (args[6].equals("0")) ? false : true;
		int method = Integer.parseInt(args[7]);
		
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
			
		System.out.println("Pattern expression: " + patternExpression);
		System.out.println("Support threshold: " + support);
		
		
		PatEx ex = new PatEx(patternExpression);
		
		// Generate cFST
		Fst cFst = ex.translateToFst();
		//cFst.minimize();
		
				
		// Generate optimized cFst
		XFst xFst = cFst.optimizeForExecution();
		
		xFst.print("/home/kbeedkar/temp/toy");
		
		DesqDfs dd = null;
		
		if(method == 1) {
			
		}
		else if(method == 0) {
			dd = new DfsOnePass(support, xFst, writeOutput);
		}

		totalTime.start();
		
		dd.scan(sequenceFile);
		dd.mine();

		totalTime.stop();
		
		// Write stats to log file
		LogWriter lwriter = LogWriter.getInstance();
		lwriter.setOutputPath(logfile);
		String s = null;
		s = dd.getClass().getSimpleName()
				+ "\t" + patternExpression
				+ "\t" + support
				+ "\t" + totalTime.elapsed(TimeUnit.SECONDS)
				+ "\t" + dd.noPatterns();
		lwriter.write(s);
			
		
	}

}
