package de.uni_mannheim.desq.examples;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.*;
import de.uni_mannheim.desq.mining.*;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.*;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DesqDfsLocalDistributedMining {

	static boolean caseSet = false;
	static long sigma;
	static String patternExp;
	static File dataFile;
	static Dictionary dict;
	static String runVersion;
	static int expNo;
	static boolean verbose;
	static String scenarioStr;
	static String useCase;
	static boolean useTransitionRepresentation;
	static boolean useTreeRepresentation;

	/** main
	 *
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		if(args.length > 0) {
			runDistributedMiningLocally(args);
		} else {
			runDistributedMiningLocally("I2", 3, 1);
		}
	}


	public static void runDistributedMiningLocally(String args[]) throws IOException {

		runVersion=args[0];

		expNo = Integer.parseInt(args[1]);
		String theCase = args[2];
		int scenario = Integer.parseInt(args[3]);
		int run = Integer.parseInt(args[4]);

		System.out.println(runVersion+" " + expNo + ": Running Distributed DesqDfs locally("+theCase+", "+scenario+", "+run+") now");
		runDistributedMiningLocally(theCase, scenario, run);
	}

	public static void runDistributedMiningLocally(String theCase, int scenario, int run) throws IOException{

		setCase(theCase);
		setScenario(scenario);

		System.out.println("------------------------------------------------------------------");
		System.out.println("Distributed Mining " + theCase + " @ " + scenarioStr + "  #" + run);
		System.out.println("------------------------------------------------------------------");

		DesqProperties minerConf = DesqDfs.createConf(patternExp, sigma);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), true);
		dataReader.setDictionary(dict);

		minerConf.setProperty("desq.mining.use.transition.representation", useTransitionRepresentation);
		minerConf.setProperty("desq.mining.use.tree.representation", useTreeRepresentation);

		// default settings
		minerConf.setProperty("desq.mining.skip.non.pivot.transitions", false);
		minerConf.setProperty("desq.mining.use.minmax.pivot", false);
		minerConf.setProperty("desq.mining.use.first.pc.version", false);
		minerConf.setProperty("desq.mining.pc.use.compressed.transitions", true);
		minerConf.setProperty("desq.mining.use.two.pass", false);

		// create context
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dataReader.getDictionary();
		PatternWriter result;
		if (verbose) result = new MemoryPatternWriter();
		else result = new CountPatternWriter();
		ctx.patternWriter = result;
		minerConf.setProperty("desq.mining.prune.irrelevant.inputs", false);

		ctx.conf = minerConf;

		ctx.conf.prettyPrint();

		// create miner
		System.out.print("Creating miner... ");
		Stopwatch prepTime = Stopwatch.createStarted();
		DesqDfs miner = (DesqDfs) DesqDfs.create(ctx);
		prepTime.stop();
		System.out.println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms");

		// Read input sequences into memory
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

		// run partition construction
		System.out.print("Determining pivot items... ");
		Stopwatch pcTime = Stopwatch.createStarted();
		Int2ObjectOpenHashMap<ObjectList<IntList>> partitions = miner.createPartitions(inputSequences, verbose);
		pcTime.stop();
		System.out.println(pcTime.elapsed(TimeUnit.MILLISECONDS) + "ms");

		// some stats (outside time)
		int numPartitions = partitions.size();
		int numShuffleSequences = 0;
		int numShuffleInts = 0;
		for(Map.Entry<Integer, ObjectList<IntList>> partition : partitions.entrySet()) {
			for(IntList sequence :  partition.getValue()) {
				numShuffleSequences++;
				numShuffleInts += sequence.size();
			}
		}


		// mine the partitions
		System.out.print("Mining... ");
		Stopwatch mineTime = Stopwatch.createStarted();
		int key;
		ObjectList<IntList> sequences;
		for(Map.Entry<Integer, ObjectList<IntList>> partition : partitions.entrySet()) {
			key = partition.getKey();
			sequences = partition.getValue();
			if(verbose) {
				System.out.println("Partition " + key + ": " + sequences.size() + " sequences:");
			}
			miner.clear();
			for(IntList sequence : sequences) {
				if(verbose) System.out.println(sequence);
				miner.addInputSequence(sequence, 1, true);
			}
			miner.minePivot(key);

		}

		mineTime.stop();
		System.out.println(mineTime.elapsed(TimeUnit.MILLISECONDS) + "ms");


		System.out.println("Total time: " +
				(prepTime.elapsed(TimeUnit.MILLISECONDS) + ioTime.elapsed(TimeUnit.MILLISECONDS) +  pcTime.elapsed(TimeUnit.MILLISECONDS) +  mineTime.elapsed(TimeUnit.MILLISECONDS)
				) + "ms");


		// print results
		System.out.println("Number of partitions: " + numPartitions);
		System.out.println("Number of shuffled lists: " + numShuffleSequences);
		System.out.println("Number of shuffled integers: " + numShuffleInts);

		long patCount;
		long patTotalFreq = 0;
		if(verbose) {
			MemoryPatternWriter pw = (MemoryPatternWriter) result;
			patCount = pw.getPatterns().size();
			for(WeightedSequence ws : pw.getPatterns()) {
				System.out.println(ws);
				patTotalFreq += ws.support;
			}
		} else {
			CountPatternWriter pw = (CountPatternWriter) result;
			patCount = pw.getCount();
			patTotalFreq = pw.getTotalFrequency();
		}
		System.out.println("Number of patterns: " + patCount);
		System.out.println("Total frequency of all patterns: " + patTotalFreq);

		// combined print
		System.out.println("exp. no\tcase\toptimizations\trun\tcreate time\tread time\tpc time\tmine time\tno. partitions\tno. shuffle lists\tno. shuffle ints\ttotal Recursions\ttrs used\tmxp used\tno. patterns\ttotal freq. patterns");
		String out = expNo + "\t" + theCase + "\t" + scenarioStr + "\t" + run + "\t" + prepTime.elapsed(TimeUnit.MILLISECONDS) + "\t" +
				ioTime.elapsed(TimeUnit.MILLISECONDS) + "\t" + pcTime.elapsed(TimeUnit.MILLISECONDS) + "\t" + mineTime.elapsed(TimeUnit.MILLISECONDS) + "\t" +
				numPartitions + "\t" + numShuffleSequences + "\t" + numShuffleInts + "\t" + miner.counterTotalRecursions + "\t" + miner.counterNonPivotTransitionsSkipped + "\t" +
				miner.counterMaxPivotUsed + "\t" + patCount + "\t" + patTotalFreq;
		System.out.println(out);

		try{
			PrintWriter writer = new PrintWriter(new FileOutputStream(new File("/home/alex/Dropbox/Master/Thesis/Experiments/F/log-"+runVersion+".txt"), true));
			writer.println(out);
			writer.close();
		} catch (Exception e) {
			System.out.println("Can't open file!");
			e.printStackTrace();
		}
	}


	private static void setCase(String thisUseCase) throws IOException {
		String dataDir;
		verbose = false;
		useCase = thisUseCase;
		switch (useCase) {
			case "N1-1991":
			case "N1":
				patternExp = "ENTITY@ (VB@+ NN@+? IN@?) ENTITY@";
				sigma = 10;
				setNytData();
				break;
			case "N2-1991":
			case "N2":
			    patternExp = "(ENTITY@^ VB@+ NN@+? IN@? ENTITY@^)";
				sigma = 100;
				setNytData();
				break;
			case "N3-1991":
			case "N3":
				patternExp = "(ENTITY@^ be@VB@=^) DT@? (RB@? JJ@? NN@)";
				sigma = 10;
				setNytData();
				break;
			case "N4-1991":
			case "N4":
				patternExp = "(.^){3} NN@";
				sigma = 1000;
				setNytData();
				break;
			case "N5":
				patternExp = "([.^ . .]|[. .^ .]|[. . .^])";
				sigma = 1000;
                setNytData();
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
			case "I1@1":
				patternExp = "[c|d]([A^|B=^]+)e";
				sigma = 1;
				verbose = true;
				setICDMData();
				break;
			case "I1@2":
				patternExp = "[c|d]([A^|B=^]+)e";
				sigma = 2;
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
		useTransitionRepresentation = false;

		switch(scenario) {
			case 1:
				scenarioStr = "send-input-sequences";
				break;
			case 2:
				scenarioStr = "send-transactions";
				useTransitionRepresentation = true;
				break;
			case 3:
				scenarioStr = "send-trees";
                useTransitionRepresentation = true;
				useTreeRepresentation = true;
				break;
			default:
				System.out.println("Unknown variant");
		}
	}

	private static void setAmznData() throws IOException {
		String dataDir = "/home/alex/Data/amzn/";
		dict = Dictionary.loadFrom(dataDir + "amzn-dict.avro.gz");
		dataFile = new File(dataDir + "amzn-data.del");
	}

	private static void setICDMData() throws IOException {
		String dataDir = "/home/alex/Data/icdm16fids/";
		dict = Dictionary.loadFrom(dataDir + "dict.json");
		dataFile  = new File(dataDir + "data.del");
	}

	private static void setNytData()  throws IOException {
		String dataset = "nyt";
		if(useCase.contains("1991")) {
			dataset = "nyt-1991";
		}
		String dataDir = "/home/alex/Data/" + dataset + "/";

		dict = Dictionary.loadFrom(dataDir + dataset + "-dict.avro.gz");
		dataFile  = new File(dataDir + dataset + "-data.del");
	}
}