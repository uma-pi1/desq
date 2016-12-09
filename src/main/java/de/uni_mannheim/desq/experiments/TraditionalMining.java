package de.uni_mannheim.desq.experiments;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.driver.DesqDfsDriver;
import de.uni_mannheim.desq.io.CountPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.PrefixGrowthMiner;
import de.uni_mannheim.desq.util.DesqProperties;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author kbeedkar {kbeedkar@uni-mannheim.de}.
 */
public class TraditionalMining {
    private static final Logger logger = Logger.getLogger(TraditionalMining.class.getSimpleName());


    public void prefixGrowth(String inputFile, String dictFile, String outDir,
                             int sigma, int gamma, int lambda, boolean generalize) throws IOException {

        Dictionary dict = Dictionary.loadFrom(dictFile);
        dict.freeze();
        SequenceReader dataReader = new DelSequenceReader(new FileInputStream(inputFile), true);
        dataReader.setDictionary(dict);

        String method = sigma + "-" + gamma + "-" + lambda + "-" + generalize;
        File outFile = new File(outDir + "/" + "PrefixGrowth" + method);
        File parentFile = outFile.getParentFile();
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
        outFile.delete();

        DesqMinerContext ctx = new DesqMinerContext();
        ctx.dict = dict;
        //DelPatternWriter patternWriter = new DelPatternWriter(new FileOutputStream(outFile), DelPatternWriter.TYPE.SID);
        //ctx.patternWriter = patternWriter;
        CountPatternWriter result = new CountPatternWriter();
        ctx.patternWriter = result;
        ctx.conf = PrefixGrowthMiner.createConf(sigma,gamma,lambda,generalize);

        Stopwatch constructionTime = Stopwatch.createStarted();
        DesqMiner miner = DesqMiner.create(ctx);
        constructionTime.stop();

        Stopwatch readInputTime = Stopwatch.createStarted();
        miner.addInputSequences(dataReader);
        readInputTime.stop();

        Stopwatch miningTime = Stopwatch.createStarted();
        miner.mine();
        miningTime.stop();

        //patternWriter.close();
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

    public static void main(String[] args) throws  IOException {
        logger.setLevel(Level.INFO);
        logger.info(Arrays.toString(args));

        String method = args[0];
        String inputFile = args[1];
        String dictFile = args[2];
        String outDir = args[3];

        int sigma = Integer.parseInt(args[4]);
        int gamma = Integer.parseInt(args[5]);
        int lambda = Integer.parseInt(args[6]);
        boolean generalize = Boolean.parseBoolean(args[7]);

        if(method=="prefixGrowth") {
            new TraditionalMining().prefixGrowth(inputFile, dictFile, outDir, sigma, gamma, lambda, generalize);
            return;
        }

        String patternExpression = DesqMiner.patternExpressionFor(gamma, lambda, generalize);

        boolean pruneIrrelevantInputs = false;
        boolean useTwoPass = false;
        if(args.length > 8) {
            pruneIrrelevantInputs = Boolean.parseBoolean(args[8]);
            useTwoPass = Boolean.parseBoolean(args[9]);
        }

        DesqDfsDriver desqDfsDriver = new DesqDfsDriver(inputFile, dictFile, outDir, patternExpression, sigma);
        desqDfsDriver.runDesqDfs(pruneIrrelevantInputs, useTwoPass);
    }
}
