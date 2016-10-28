package de.uni_mannheim.desq.examples;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.CountPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.util.DesqProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DesqDfsLocalRun {
	public static void nyt() throws IOException {
		int sigma = 10;
		int gamma = 0;
		int lambda = 3;
		boolean generalize = true;
		String patternExp = DesqDfs.patternExpressionFor(gamma, lambda, generalize);
		patternExp = "(JJ@ JJ@ NN@)";

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
		// conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		ExampleUtils.runNyt(conf);
	}

	public static void icdm16(String[] args) throws IOException {
		
		
		String patternExp= "[c|d]([A^|B=^]+)e";
		int sigma = 2;

		if(args.length >= 2) {
			sigma = Integer.parseInt(args[0]);
			patternExp = args[1];
		}

		//patternExp= "(a1)..$";
		//sigma = 1;

		//patternExp= "^.(a1)";
		//sigma = 1;

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.two.pass", false);
		ExampleUtils.runIcdm16(conf);
	}

	public static void netflixFlat() throws IOException {
		String patternExp= "(.)";
        int sigma = 100000;
		// these patterns are all spurious due to the way the data is created (ratings on same day ordered by id)
		patternExp="(.).{0,3}(.).{0,3}('The Incredibles#2004#10947')";
		sigma = 1000;

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.two.pass", true);
		ExampleUtils.runNetflixFlat(conf);
	}

    public static void netflixDeep() throws IOException {
        String patternExp= "(.)";
        int sigma = 100000;

        // these patterns are all spurious due to the way the data is created (ratings on same day ordered by id)
        patternExp="(.).{0,3}(.).{0,3}('The Incredibles#2004#10947'=^)";
        sigma = 1000;
        patternExp="('5stars'{2})";
        sigma = 10000;

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
        conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
        conf.setProperty("desq.mining.use.two.pass", true);
        ExampleUtils.runNetflixDeep(conf);
    }

    public static void runPartitionConstruction() throws IOException {
		long sigma = 500;
		String patternExp = "(Electronics^)[.{0,2}(Electronics^)]{1,4}";


		DesqProperties minerConf = DesqDfs.createConf(patternExp, sigma);


		String dataDir = "/home/alex/Data/amzn/";
		Dictionary dict = Dictionary.loadFrom(dataDir + "amzn-dict.avro.gz");
		File dataFile = new File(dataDir + "amzn-data.del");
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), true);
		dataReader.setDictionary(dict);

		// create context
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dataReader.getDictionary();
		CountPatternWriter result = new CountPatternWriter();
		ctx.patternWriter = result;
		ctx.conf = minerConf;

		// perform the mining
		System.out.print("Creating miner... ");
		Stopwatch prepTime = Stopwatch.createStarted();
		DesqDfs miner = (DesqDfs) DesqDfs.create(ctx);
		prepTime.stop();
		System.out.println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms");

		System.out.print("Reading input sequences... ");
		Stopwatch ioTime = Stopwatch.createStarted();
		miner.determinePivotElementsForSequences(dataReader);
		ioTime.stop();
		System.out.println(ioTime.elapsed(TimeUnit.MILLISECONDS) + "ms");

		/*
		System.out.print("Mining... ");
		Stopwatch miningTime = Stopwatch.createStarted();
		miner.mine();
		miningTime.stop();
		System.out.println(miningTime.elapsed(TimeUnit.MILLISECONDS) + "ms");
		*/

		System.out.println("Total time: " +
				(prepTime.elapsed(TimeUnit.MILLISECONDS) + ioTime.elapsed(TimeUnit.MILLISECONDS)
						) + "ms");

		// print results
		System.out.println("Number of patterns: " + result.getCount());
		System.out.println("Total frequency of all patterns: " + result.getTotalFrequency());
	}

	public static void main(String[] args) throws IOException {
		runPartitionConstruction();
	}

	public static void runMining() throws IOException {

		// Run N5
		//N5 String patternExp = "([.^ . .]|[. .^ .]|[. . .^])";

        // A1
        long sigma = 500;
        String patternExp = "(Electronics^)[.{0,2}(Electronics^)]{1,4}";


		DesqProperties minerConf = DesqDfs.createConf(patternExp, sigma);
		// conf.setProperty("desq.mining.prune.irrelevant.inputs", true);


		/*String dataDir = "/home/alex/Data/nyt/";
		Dictionary dict = Dictionary.loadFrom(dataDir + "nyt-dict.avro.gz");
		File dataFile = new File(dataDir + "nyt-data.del");
		SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), true);
		dataReader.setDictionary(dict);*/

        String dataDir = "/home/alex/Data/amzn/";
        Dictionary dict = Dictionary.loadFrom(dataDir + "amzn-dict.avro.gz");
        File dataFile = new File(dataDir + "amzn-data.del");
        SequenceReader dataReader = new DelSequenceReader(new FileInputStream(dataFile), true);
        dataReader.setDictionary(dict);

		// create context
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.dict = dataReader.getDictionary();
		CountPatternWriter result = new CountPatternWriter();
		ctx.patternWriter = result;
		ctx.conf = minerConf;

		// perform the mining
		DesqMiner miner = ExampleUtils.runMiner(dataReader, ctx);

		// print results
		System.out.println("Number of patterns: " + result.getCount());
		System.out.println("Total frequency of all patterns: " + result.getTotalFrequency());


		//icdm16(args);
		//nyt();
		//netflixFlat();
        //netflixDeep();
	}
}
