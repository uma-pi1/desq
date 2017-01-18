package de.uni_mannheim.desq.examples;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.*;
import de.uni_mannheim.desq.mining.*;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import scala.Tuple2;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class DesqDfsRunDistributedMiningLocally {

	static long sigma;
	static String patternExp;
	static File dataFile;
	static Dictionary dict;
	static int expNo;
	public static int scenario;

	static boolean sendNFAs;
	static boolean mergeSuffixes;
	static boolean useDesqCount;
	static boolean useTwoPass;
	static int 	   maxNumOutputItems;

	public static String useCase;
	static String baseFolder;
	static String scenarioStr;
	static String runVersion;

	static PrintWriter statsWriter;

	public static boolean verbose;
	public static boolean writeShuffleStats = false;
	public static boolean drawGraphs = false;

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
		    drawGraphs = false;
			runDistributedMiningLocally(args);
		} else {
			localCorrectnessTest(); System.exit(0);
			runDistributedMiningLocally("A1", 2, 1);
		}
	}

	public static void localCorrectnessTest() throws IOException {
		String[] tests = {"I1@1", "I1@2", "I2", "IA2", "IA4", "IX1", "IX2", "IX3", "IX4"};
//		String[] tests = {"N1", "N2", "N3", "N4", "N5", "A1", "A2", "A3", "A4"};
		int[] scenarios = {0, 2};

		String output = "";
		for (String testCase : tests) {
			for (int scenario : scenarios) {
				Tuple2<Long, Long> res = runDistributedMiningLocally(testCase, scenario, 1);
				output += testCase + " // " + scenario + "\t" + res._1() + "\t" + res._2() + "\n";
			}
			output += "\n";
		}

		System.out.println("###############################################################");
		System.out.println("###############################################################");
		System.out.println(output);
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


	public static Tuple2<Long,Long> runDistributedMiningLocally(String theCase, int scenario, int run) throws IOException {
		if(scenario == 0)
			return runDesqCountDistributedLocally(theCase, scenario, run);
		else
			return runDesqDfsDistributedLocally(theCase, scenario, run);
	}

	public static  Tuple2<Long,Long> runDesqDfsDistributedLocally(String theCase, int scenario, int run) throws IOException {
		setCase(theCase);
		setScenario(scenario);

		System.out.println("------------------------------------------------------------------");
		System.out.println("Distributed Mining " + theCase + " @ " + scenarioStr + "  #" + run);
		System.out.println("------------------------------------------------------------------");

		DesqProperties minerConf = DesqDfs.createConf(patternExp, sigma);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), true);
		dataReader.setDictionary(dict);

		minerConf.setProperty("desq.mining.send.nfas", sendNFAs);
        minerConf.setProperty("desq.mining.merge.suffixes", mergeSuffixes);
		minerConf.setProperty("desq.mining.use.two.pass", useTwoPass);
		minerConf.setProperty("desq.mining.shuffle.max.num.output.items", maxNumOutputItems);

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
		Stopwatch prepTime = new Stopwatch().start();
		DesqDfs miner = (DesqDfs) DesqDfs.create(ctx);
		prepTime.stop();
		System.out.println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms");

		// Read input sequences into memory
		System.out.print("Reading input sequences into memory... ");
		Stopwatch ioTime = new Stopwatch().start();
		ObjectArrayList<Sequence> inputSequences = new ObjectArrayList<Sequence>();
		Sequence inputSequence = new Sequence();
		while (dataReader.readAsFids(inputSequence)) {
			inputSequences.add(inputSequence);
			inputSequence = new Sequence();
		}
		ioTime.stop();
		System.out.println(ioTime.elapsed(TimeUnit.MILLISECONDS) + "ms");

		// run partition construction
		if(writeShuffleStats) openStatsWriter();
		System.out.print("Determining pivot items... ");
		Stopwatch pcTime = new Stopwatch().start();
		Int2ObjectOpenHashMap<Object2IntOpenHashMap<Sequence>> partitions = new Int2ObjectOpenHashMap<>();
		int seqNo = 0;
		for(Sequence inputSeq : inputSequences) {
			miner.generateOutputNFAs(inputSeq, partitions, seqNo++);
		}
		pcTime.stop();
		System.out.println(pcTime.elapsed(TimeUnit.MILLISECONDS) + "ms");
		if(writeShuffleStats) closeStatsWriter();


		// clear the memory
		inputSequences.clear();
		inputSequences.trim();

		// some stats (outside time)
		int numPartitions = partitions.size();
		int numShuffleSequences = 0;
		int numShuffleInts = 0;
		for(Int2ObjectMap.Entry<Object2IntOpenHashMap<Sequence>> partition : partitions.int2ObjectEntrySet()) {
			for(Object2IntMap.Entry<Sequence> weightedNFA:  partition.getValue().object2IntEntrySet()) {
				numShuffleSequences++;
				numShuffleInts += weightedNFA.getKey().size();
			}
		}


		// mine the partitions
		System.out.print("Mining... ");
		Stopwatch mineTime = new Stopwatch().start();
		int key;
		Object2IntOpenHashMap<Sequence> nfas;
		for(Int2ObjectMap.Entry<Object2IntOpenHashMap<Sequence>> partition : partitions.int2ObjectEntrySet()) {
			key = partition.getIntKey();
            nfas = partition.getValue();
			if(verbose) {
				System.out.println("Partition " + key + ": " + nfas.size() + " sequences:");
			}
			miner.clear();
			for(Object2IntMap.Entry<Sequence> weightedNFA:  partition.getValue().object2IntEntrySet()) {
				if(verbose) System.out.println(weightedNFA.getKey());
				if(sendNFAs)
					miner.addNFA(weightedNFA.getKey(), weightedNFA.getIntValue(), true, key);
				else
					miner.addInputSequence(weightedNFA.getKey(), weightedNFA.getIntValue(), true);
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
//				System.out.println(ws);
				patTotalFreq += ws.weight;
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
				numPartitions + "\t" + numShuffleSequences + "\t" + numShuffleInts + "\t" + miner.counterTotalRecursions + "\t" +
				patCount + "\t" + patTotalFreq;
		System.out.println(out);

		try{
			PrintWriter writer = new PrintWriter(new FileOutputStream(new File(baseFolder + "Dropbox/Master/Thesis/Experiments/F/log-"+runVersion+".txt"), true));
			writer.println(out);
			writer.close();
		} catch (Exception e) {
			System.out.println("Can't open file!");
			e.printStackTrace();
		}

		try{
			PrintWriter writer = new PrintWriter(new FileOutputStream(new File(baseFolder + "Dropbox/Master/Thesis/Experiments/H/timings-"+runVersion+".txt"), true));
			String timings = expNo + "\t" + theCase + "\t" + scenarioStr + "\t" + run + "\t" +
					miner.swFirstPass.elapsed(TimeUnit.MILLISECONDS) + "\t" +
					miner.swSecondPass.elapsed(TimeUnit.MILLISECONDS) + "\t" +
					miner.swPrep.elapsed(TimeUnit.MILLISECONDS) + "\t" +
					miner.swSetup.elapsed(TimeUnit.MILLISECONDS) + "\t" +
					miner.swTrim.elapsed(TimeUnit.MILLISECONDS) + "\t" +
					miner.swMerge.elapsed(TimeUnit.MILLISECONDS) + "\t" +
					miner.swSerialize.elapsed(TimeUnit.MILLISECONDS) + "\t" +
					miner.swReplace.elapsed(TimeUnit.MILLISECONDS) + "\t" +
					miner.maxNumStates + "\t" +
					miner.maxRelevantSuccessors + "\t" +
					miner.counterTrimCalls + "\t" +
					miner.counterFollowGroupCalls + "\t" +
					miner.counterIsMergeableIntoCalls + "\t" +
					miner.counterFollowTransitionCalls + "\t" +
					miner.counterTransitionsCreated + "\t" +
					miner.counterSerializedStates + "\t" +
					miner.counterSerializedTransitions + "\t" +
					miner.counterPathsAdded + "\t" +
					miner.maxFollowGroupSetSize + "\t" +
					miner.maxPivotsForOneSequence + "\t" +
					miner.maxPivotsForOnePath + "\t" +
					miner.maxNumOutTrs + "\t" +
					miner.counterPrunedOutputs + "\t" +
					miner.maxNumOutputItems + "\t" +
					"";

			writer.println(timings);
			writer.close();
		} catch (Exception e) {
			System.out.println("Can't open file!");
			e.printStackTrace();
		}

		return new Tuple2(patCount, patTotalFreq);
	}


	public static Tuple2<Long,Long> runDesqCountDistributedLocally(String theCase, int scenario, int run) throws IOException{

		setCase(theCase);
		setScenario(scenario);

		System.out.println("------------------------------------------------------------------");
		System.out.println("Distributed Mining " + theCase + " @ " + scenarioStr + "  #" + run);
		System.out.println("------------------------------------------------------------------");

		DesqProperties minerConf = DesqCount.createConf(patternExp, sigma);
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), true);
		dataReader.setDictionary(dict);

		minerConf.setProperty("desq.mining.use.two.pass", useTwoPass);

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
		Stopwatch prepTime = new Stopwatch().start();
		DesqCount miner = (DesqCount) DesqDfs.create(ctx);
		prepTime.stop();
		System.out.println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms");

		// Read input sequences into memory
		System.out.print("Reading input sequences into memory... ");
		Stopwatch ioTime = new Stopwatch().start();
		ObjectArrayList<Sequence> inputSequences = new ObjectArrayList<Sequence>();
		Sequence inputSequence = new Sequence();
		while (dataReader.readAsFids(inputSequence)) {
			inputSequences.add(inputSequence);
			inputSequence = new Sequence();
		}
		ioTime.stop();
		System.out.println(ioTime.elapsed(TimeUnit.MILLISECONDS) + "ms");

		// run partition construction
		if(writeShuffleStats) openStatsWriter();
		System.out.print("Adding input sequences to miner ...");
		Stopwatch pcTime = new Stopwatch().start();
		for(IntList sequence : inputSequences) {
			miner.addInputSequence(sequence, 1, true);
		}
		System.out.println(pcTime.elapsed(TimeUnit.MILLISECONDS) + "ms");
		if(writeShuffleStats) closeStatsWriter();

		// clear the memory
		inputSequences.clear();
		inputSequences.trim();


		// mine the partitions
		System.out.print("Mining... ");
		Stopwatch mineTime = new Stopwatch().start();
		miner.mine();
		mineTime.stop();
		System.out.println(mineTime.elapsed(TimeUnit.MILLISECONDS) + "ms");


		System.out.println("Total time: " +
				(prepTime.elapsed(TimeUnit.MILLISECONDS) + ioTime.elapsed(TimeUnit.MILLISECONDS) +  pcTime.elapsed(TimeUnit.MILLISECONDS) +  mineTime.elapsed(TimeUnit.MILLISECONDS)
				) + "ms");



		long patCount;
		long patTotalFreq = 0;
		if(verbose) {
			MemoryPatternWriter pw = (MemoryPatternWriter) result;
			patCount = pw.getPatterns().size();
			for(WeightedSequence ws : pw.getPatterns()) {
//				System.out.println(ws);
				patTotalFreq += ws.weight;
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
				0 + "\t" + 0 + "\t" + 0 + "\t" + 0 + "\t" + 0 + "\t" +
				0 + "\t" + patCount + "\t" + patTotalFreq;
		System.out.println(out);

		try{
			PrintWriter writer = new PrintWriter(new FileOutputStream(new File(baseFolder + "Dropbox/Master/Thesis/Experiments/H/log-"+runVersion+".txt"), true));
			writer.println(out);
			writer.close();
		} catch (Exception e) {
			System.out.println("Can't open file!");
			e.printStackTrace();
		}

		return new Tuple2(patCount, patTotalFreq);
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
				if(useCase.contains("1991")) sigma = sigma / 10;
				setNytData();
				break;
			case "N2-1991":
			case "N2":
			    patternExp = "(ENTITY@^ VB@+ NN@+? IN@? ENTITY@^)";
				sigma = 100;
				if(useCase.contains("1991")) sigma = sigma / 10;
				setNytData();
				break;
			case "N3-1991":
			case "N3":
				patternExp = "(ENTITY@^ be@VB@=^) DT@? (RB@? JJ@? NN@)";
				sigma = 10;
				if(useCase.contains("1991")) sigma = sigma / 10;
				setNytData();
				break;
			case "N4-1991":
			case "N4":
				patternExp = "(.^){3} NN@";
				sigma = 1000;
				if(useCase.contains("1991")) sigma = sigma / 10;
				setNytData();
				break;
			case "N5-1991":
			case "N5":
				patternExp = "([.^ . .]|[. .^ .]|[. . .^])";
				sigma = 1000;
				if(useCase.contains("1991")) sigma = sigma / 10;
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
			case "IX1":
				patternExp = "[c|d](a2).*([A^|B=^]).*(e)";
				sigma = 1;
				verbose = true;
				setICDMData();
				break;
			case "IX2":
				patternExp = "[c|d](a2).*([A^|B=^]).*(B^e)";
				sigma = 1;
				verbose = true;
				setICDMData();
				break;
			case "IX3":
				patternExp = "(a1* b12 e)";
				sigma = 1;
				verbose = true;
				setICDMData();
				break;
			case "IX4":
				patternExp = "([c|a1] .* [.* A]+ .* [d|e])";
				sigma = 1;
				verbose = true;
				setICDMData();
				break;
			default:
				System.out.println("Do not know the use case " + useCase);
				System.exit(1);
		}
	}

	private static void setScenario(int setScenario) {
		//set some defaults
		scenario = setScenario;
        sendNFAs = false;
		mergeSuffixes = false;
		useDesqCount = false;
		useTwoPass = false;
		maxNumOutputItems = 2;
		switch(scenario) {
			case 0:
				scenarioStr = "Count, shuffle output sequences";
				useDesqCount = true;
				useTwoPass = true;
				break;
			case 1:
				scenarioStr = "Dfs, shuffle input sequences";
				useTwoPass = true;
				break;
			case 2:
				scenarioStr = "Dfs, DAGs. pNFAg";
                sendNFAs = true;
				useTwoPass = true;
				break;
			default:
				System.out.println("Unknown variant");
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

	private static void setNytData()  throws IOException {
		String dataset = "nyt";
		if(useCase.contains("1991")) {
			dataset = "nyt-1991";
		}
		String dataDir = baseFolder + "Data/" + dataset + "/";

		dict = Dictionary.loadFrom(dataDir + dataset + "-dict.avro.gz");
		dataFile  = new File(dataDir + dataset + "-data.del");
	}

	public static void openStatsWriter() {
		try{
			statsWriter = new PrintWriter(new FileOutputStream(new File(baseFolder + "Dropbox/Master/Thesis/Experiments/F/numSerializedStates-"+runVersion+".txt"), true));
		} catch (Exception e) {
			System.out.println("Can't open file to write shuffle stats!");
			e.printStackTrace();
		}
	}

	public static void closeStatsWriter() {
        statsWriter.close();
	}

	public static void writeShuffleStats(int seqNo, int pivotItem, int numStates) {
		statsWriter.println(useCase + "\t" + scenario + "\t" + seqNo + "\t" + pivotItem + "\t" + numStates);
	}
}