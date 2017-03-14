package de.uni_mannheim.desq.driver;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.CountPatternWriter;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author kbeedkar {kbeedkar@uni-mannheim.de}.
 */
public class DesqDfsDriver extends Driver {

    private static final Logger logger = Logger.getLogger(DesqDfsDriver.class.getSimpleName());

    public DesqDfsDriver(String inputFile, String dicFile, String outDir, String patterExpression, int sigma) {
        super(inputFile, dicFile, outDir, patterExpression, sigma);
    }

    public void runDesqDfs(boolean pruneIrrelevantInputs, boolean useTwoPass) throws IOException {
        logger.setLevel(Level.INFO);
        // Set IO paths
        Dictionary dict = Dictionary.loadFrom(dictFile);
        dict.freeze();
        SequenceReader dataReader = new DelSequenceReader(new FileInputStream(inputFile), true);
        dataReader.setDictionary(dict);

        String method = toLetter(pruneIrrelevantInputs)+toLetter(useTwoPass)+"-"+sanitize(patternExpression)+"-"+sigma;
        File outFile = new File(outDir + "/" + "DesqDfs-" + method);
        File parentFile = outFile.getParentFile();
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
        outFile.delete();

        DesqMinerContext ctx = new DesqMinerContext();
        ctx.dict = dict;
//        DelPatternWriter patternWriter = new DelPatternWriter(new FileOutputStream(outFile), DelPatternWriter.TYPE.SID);
//        patternWriter.setDictionary(dict);
//        ctx.patternWriter = patternWriter;
        CountPatternWriter result = new CountPatternWriter();
        ctx.patternWriter = result;

        ctx.conf = DesqDfs.createConf(patternExpression, sigma);
        ctx.conf.setProperty("desq.mining.prune.irrelevant.inputs", pruneIrrelevantInputs);
        ctx.conf.setProperty("desq.mining.use.two.pass", useTwoPass);
        ctx.conf.setProperty("desq.mining.use.lazy.dfa", false);

        Stopwatch constructionTime = Stopwatch.createStarted();
        DesqMiner miner = DesqMiner.create(ctx);
        constructionTime.stop();

        Stopwatch readInputTime = Stopwatch.createStarted();
        miner.addInputSequences(dataReader);
        readInputTime.stop();

        Stopwatch miningTime = Stopwatch.createStarted();
        miner.mine();
        miningTime.stop();

//        patternWriter.close();
        result.close();


        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(method + ", ");
        stringBuilder.append(constructionTime.elapsed(TimeUnit.SECONDS) + ", ");
        stringBuilder.append(readInputTime.elapsed(TimeUnit.SECONDS) + ", ");
        stringBuilder.append(miningTime.elapsed(TimeUnit.SECONDS) + ", ");
        stringBuilder.append(result.getCount() + ", ");
        stringBuilder.append(result.getTotalFrequency());

        logger.info(stringBuilder.toString());

    }

    public static void main(String[] args) throws IOException {
        // get command line options
        if (args.length != 7) {
            System.out.println("usage: inputFile dictFile outDir \"patternExpression\" sigma pruneIrrelevantInputs useTwoPass");
            System.exit(-1);
        }
        String inputFile = args[0];
        String dictFile = args[1];
        String outDir = args[2];
        String patternExpression = args[3];
        int sigma = Integer.parseInt(args[4]);
        boolean pruneIrrelevantInputs = Boolean.parseBoolean(args[5]);
        boolean useTwoPass = Boolean.parseBoolean(args[6]);

        DesqDfsDriver ddd = new DesqDfsDriver(inputFile, dictFile, outDir, patternExpression, sigma);

        logger.setLevel(Level.INFO);
        logger.info(Arrays.toString(args));
        ddd.runDesqDfs(pruneIrrelevantInputs, useTwoPass);

    }
}
