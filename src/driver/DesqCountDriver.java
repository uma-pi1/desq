package driver;

import fst.XFst;
import fst.Fst;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import patex.PatEx;
import mining.DesqCount;
//import mining.OnePassIterative;
import mining.OnePassRecursive;
//import mining.TwoPass;
import utils.Dictionary;
import writer.LogWriter;
import writer.SequentialWriter;

/**
 * DesqCountDriver.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class DesqCountDriver {
	
	// Timers
	public static Stopwatch forwardPassTime = Stopwatch.createUnstarted();
	public static Stopwatch backwardPassTime = Stopwatch.createUnstarted();
	public static Stopwatch outputGenerationTime = Stopwatch.createUnstarted();
	public static Stopwatch fstTime = Stopwatch.createUnstarted();
	public static Stopwatch totalTime = Stopwatch.createUnstarted();
	
	
	/** <input> <output> <pattern> <support> <logfile> <writeOutput:0/1> <useflist: 0/1> <method id: 0/1>
	 * @throws Exception */
	public static void main(String[] args) throws Exception {
		System.out.println("DESQ-COUNT : " + Arrays.toString(args));
		String input = args[0];
		String output = args[1];
		String patternExpression = args[2];
		patternExpression = ".* [" + patternExpression.trim() + "]";
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
		
		
//		cFst.print("/home/kbeedkar/temp/test-repeat");
//		System.exit(0);
//		
				
		// Generate optimized cFst
		XFst xFst = cFst.optimizeForExecution();
		
		//xFst.print("/home/kbeedkar/temp/xfst.pdf");
		
		//xFst.printDIndex();
		
		DesqCount dc = null;
		
		//xPFst.print("/home/kbeedkar/temp/xPFST.pdf");
		
		// Two pass
		if(method == 1) {
			// Generate optimized reverse pFst with the same state ids
			//pFst.reverse();
			//XFst rXPFst= pFst.optimizeForExecution(false);
			
			//dc = new TwoPass(support, xPFst, writeOutput, useFlist, rXPFst);
		}
		else if(method == 0) {
			dc = new OnePassRecursive(support, xFst, writeOutput, useFlist);
		}
		else if (method == 2) {
			//dc = new OnePassIterative(support, xFst, writeOutput, useFlist);
		}
		
		//long tS = System.currentTimeMillis();
		
		totalTime.start();
		
		dc.scan(sequenceFile);
		
		totalTime.stop();
		
		//long tE = System.currentTimeMillis();
		//long totaltime = (long) ((tE-tS)/1000.0);
		
		double avgGpt = (double) dc.getGlobalGpt()/dc.getTotalMatchedSequences();
		double avgGptUnique = (double) dc.getGlobalGptUnique()/dc.getTotalMatchedSequences();
		
		/** Write stats to log*/
		//method \t pattern \t support \ totaltime
		LogWriter lwriter = LogWriter.getInstance();
		lwriter.setOutputPath(logfile);
		String s = null;
		s = dc.getClass().getSimpleName()
		+ "\t" + patternExpression 
		+ "\t" + support
		+ "\t" + totalTime.elapsed(TimeUnit.SECONDS)
		+ "\t" + dc.noOutputPatterns()
		+ "\t" + dc.getTotalMatches()
		+ "\t" + String.format("%.2f", avgGpt)
		+ "\t" + String.format("%.2f", avgGptUnique)
		+ "\t" + forwardPassTime.elapsed(TimeUnit.MILLISECONDS)
		+ "\t" + backwardPassTime.elapsed(TimeUnit.MILLISECONDS)
		+ "\t" + outputGenerationTime.elapsed(TimeUnit.MILLISECONDS)
		+ "\t" + fstTime.elapsed(TimeUnit.MILLISECONDS);
		lwriter.write(s);

	}

}
