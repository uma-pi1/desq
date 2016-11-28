package de.uni_mannheim.desq.examples;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.CountPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.Sequence;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import scala.Tuple2;

import java.io.*;

import java.util.concurrent.TimeUnit;

public class DesqDfsRunPCLocally {

	static boolean caseSet = false;
	static long sigma;
	static String patternExp;
	static File dataFile;
	static Dictionary dict;
	static boolean useFirstVersion;
	static boolean skipNonPivotTransitions;
	static boolean useMaxPivot;
	static boolean useCompressedTransitions;
	static boolean useTwoPass;
	static String runVersion;
	static int expNo;
	static boolean verbose;
	static String scenarioStr;
	static String baseFolder;



	public static void runPartitionConstruction(String args[]) throws IOException {

		runVersion=args[0];

		expNo = Integer.parseInt(args[1]);
		String theCase = args[2];
		int scenario = Integer.parseInt(args[3]);
		int run = Integer.parseInt(args[4]);

		System.out.println(runVersion+" " + expNo + ": Running runPartitionConstruction("+theCase+", "+scenario+", "+run+") now");
		runPartitionConstruction(theCase, scenario, run);
	}

	public static Tuple2<Integer,Integer> runPartitionConstruction(String theCase, int scenario, int run) throws IOException {


		setCase(theCase);
		setScenario(scenario);

		System.out.println("------------------------------------------------------------------");
		System.out.println("Running " + theCase + " @ " + scenarioStr + "  #" + run);
		System.out.println("------------------------------------------------------------------");

		DesqProperties minerConf = DesqDfs.createConf(patternExp, sigma);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), true);
		dataReader.setDictionary(dict);



		// experiment
		minerConf.setProperty("desq.mining.skip.non.pivot.transitions", skipNonPivotTransitions);
		minerConf.setProperty("desq.mining.use.minmax.pivot", useMaxPivot);
		minerConf.setProperty("desq.mining.use.first.pc.version", useFirstVersion);
		minerConf.setProperty("desq.mining.pc.use.compressed.transitions", useCompressedTransitions);
		minerConf.setProperty("desq.mining.use.two.pass", useTwoPass);



		// create context
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dataReader.getDictionary();
		CountPatternWriter result = new CountPatternWriter();
		ctx.patternWriter = result;
		minerConf.setProperty("desq.mining.prune.irrelevant.inputs", false);

		ctx.conf = minerConf;

		ctx.conf.prettyPrint();

		// perform the mining
		System.out.print("Creating miner... ");
		Stopwatch prepTime = Stopwatch.createStarted();
		DesqDfs miner = (DesqDfs) DesqDfs.create(ctx);
		prepTime.stop();
		System.out.println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms");

		System.out.print("Reading input sequences into memory... ");
		Stopwatch ioTime = Stopwatch.createStarted();
		ObjectArrayList<Sequence> inputSequences = new ObjectArrayList<Sequence>();
		Sequence inputSequence = new Sequence();
		while (dataReader.readAsFids(inputSequence)) {
			inputSequences.add(inputSequence);
			inputSequence = new Sequence();
		}
		ioTime.stop();
		System.out.println(ioTime.elapsed(TimeUnit.MILLISECONDS) + "ms");


		System.out.print("Determining pivot items... ");
		Stopwatch miningTime = Stopwatch.createStarted();
		Tuple2<Integer, Integer> stats = miner.determinePivotItemsForSequences(inputSequences, verbose);
		miningTime.stop();
		System.out.println(miningTime.elapsed(TimeUnit.MILLISECONDS) + "ms");


		System.out.println("Total time: " +
				(prepTime.elapsed(TimeUnit.MILLISECONDS) + ioTime.elapsed(TimeUnit.MILLISECONDS) +  miningTime.elapsed(TimeUnit.MILLISECONDS)
				) + "ms");


		// print results
		System.out.println("Number of sequences: " + stats._1);
		System.out.println("Total frequency of all pivot items: " + stats._2);

		// combined print
		System.out.println("exp. no, case, optimizations, run, create time, read time, process time, no. seq, no. piv, total Recursions, trs used, mxp used");
		String out = expNo + "\t" + theCase + "\t" + scenarioStr + "\t" + run + "\t" + prepTime.elapsed(TimeUnit.MILLISECONDS) + "\t" + ioTime.elapsed(TimeUnit.MILLISECONDS) + "\t" + miningTime.elapsed(TimeUnit.MILLISECONDS) + "\t" +
				stats._1 + "\t" + stats._2 + "\t" + miner.counterTotalRecursions + "\t" + miner.counterNonPivotTransitionsSkipped + "\t" + miner.counterMaxPivotUsed;
		System.out.println(out);

		try{
			PrintWriter writer = new PrintWriter(new FileOutputStream(new File(baseFolder +"Dropbox/Master/Thesis/Experiments/E/runlog-"+runVersion+".txt"), true));
			writer.println(out);
			writer.close();
		} catch (Exception e) {
			// do something
			System.out.println("Can't open file!");
			e.printStackTrace();
		}

		return stats;
	}


	private static void setCase(String useCase) throws IOException {
		String dataDir;
		verbose = false;
		switch (useCase) {
			case "N5":
				patternExp = "([.^ . .]|[. .^ .]|[. . .^])";
				sigma = 1000;
				dataDir = "/home/alex/Data/nyt/";
				dict = Dictionary.loadFrom(dataDir + "nyt-dict.avro.gz");
				dataFile  = new File(dataDir + "nyt-data.del");
				break;
			case "A1":
				patternExp = "(Electronics^)[.{0,2}(Electronics^)]{1,4}";
				sigma = 500;
				setAmznData();
				break;
			case "A2":
				patternExp = "(Books)[.{0,2}(Books)]{1,4}";
				sigma = 100;
				setAmznData();
				break;
			case "A3":
				patternExp = "Digital_Cameras@Electronics[.{0,3}(.^)]{1,4}";
				sigma = 100;
				setAmznData();
				break;
			case "A4":
				patternExp = "(Musical_Instruments^)[.{0,2}(Musical_Instruments^)]{1,4}";
				sigma = 100;
				setAmznData();
				break;
			case "I1":
				patternExp = "[c|d]([A^|B=^]+)e";
				sigma = 1;
				verbose = true;
				setICDMData();
				break;
			case "I2":
				patternExp = "([.^ . .])";
				sigma = 1;
				verbose = true;
				setICDMData();
				break;
			case "IA2":
				patternExp = "(A)[.{0,2}(A)]{1,4}";
				sigma = 1;
				verbose = true;
				setICDMData();
				break;
			case "IA4":
				patternExp = "(A^)[.{0,2}(A^)]{1,4}";
				sigma = 1;
				verbose = true;
				setICDMData();
				break;
		}
	}

	private static void setScenario(int scenario) {
		//set some defaults
		useTwoPass = false;
		useFirstVersion = false;
		skipNonPivotTransitions = false;
		useMaxPivot = false;
		useCompressedTransitions = false;
		switch(scenario) {
			case 0:
				scenarioStr = "first";
				useFirstVersion = true;
				break;
			case 1:
				scenarioStr = "pivot";
				break;
			case 2:
				scenarioStr = "pivot, trs";
				skipNonPivotTransitions = true;
				break;
			case 3:
				scenarioStr = "pivot, trs+mxp";
				skipNonPivotTransitions = true;
				useMaxPivot = true;
				break;
			case 4:
				scenarioStr = "compressed, heap";
				useCompressedTransitions = true;
				break;
			case 5:
				scenarioStr = "two-pass, uncompressed";
				useTwoPass = true;
				break;
			case 6:
				scenarioStr = "two-pass, compressed";
				useTwoPass = true;
				useCompressedTransitions = true;
				break;
			default:
				System.out.println("Unknown scenario");
		}
	}

	private static void setAmznData() throws IOException {
		String dataDir = baseFolder + "Data/amzn/";
		dict = Dictionary.loadFrom(dataDir + "amzn-dict.avro.gz");
		dataFile = new File(dataDir + "amzn-data.del");
	}

	private static void setICDMData() throws IOException {
		String dataDir = baseFolder + "Data/icdm16fids/";
		dict = Dictionary.loadFrom(dataDir + "dict.json");
		dataFile  = new File(dataDir + "data.del");
	}

	public static void localCorrectnessTest() throws IOException {
		String[] tests = {"I1", "I2", "IA2", "IA4"};
		int[] scenarios = {0, 1, 2, 3, 4, 5, 6};

		String output = "";
		for (String testCase : tests) {
			for (int scenario : scenarios) {
				Tuple2<Integer, Integer> res = runPartitionConstruction(testCase, scenario, 1);
				output += testCase + " // " + scenario + "\t" + res._1() + "\t" + res._2() + "\n";
			}
			output += "\n";
		}

		System.out.println("###############################################################");
		System.out.println("###############################################################");
		System.out.println(output);
	}


	/** main
	 *
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		if(System.getProperty("os.name").startsWith("Mac")) {
			baseFolder = "/Users/alex/";
		} else {
			baseFolder = "/home/alex/";
		}

		if(args.length > 0) {
			runPartitionConstruction(args);
		} else {
		    localCorrectnessTest(); System.exit(0);
			runPartitionConstruction("I1", 6, 1);
		}
	}
}