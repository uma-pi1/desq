package de.uni_mannheim.desq.experiments;

import de.uni_mannheim.desq.driver.DesqDfsDriver;
import de.uni_mannheim.desq.mining.DesqMiner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author kbeedkar {kbeedkar@uni-mannheim.de}.
 */
public class TraditionalMining {
    private static final Logger logger = Logger.getLogger(TraditionalMining.class.getSimpleName());

    public static void main(String[] args) throws  IOException {
        logger.setLevel(Level.INFO);
        String inputFile = args[0];
        String dictFile = args[1];
        String outDir = args[2];

        int sigma = Integer.parseInt(args[3]);
        int gamma = Integer.parseInt(args[4]);
        int lambda = Integer.parseInt(args[5]);
        boolean generalize = Boolean.parseBoolean(args[6]);
        String patternExpression = DesqMiner.patternExpressionFor(gamma, lambda, generalize);

        boolean pruneIrrelevantInputs = false;
        boolean useTwoPass = false;
        if(args.length > 7) {
            pruneIrrelevantInputs = Boolean.parseBoolean(args[7]);
            useTwoPass = Boolean.parseBoolean(args[8]);
        }

        logger.info(Arrays.toString(args));

        DesqDfsDriver desqDfsDriver = new DesqDfsDriver(inputFile, dictFile, outDir, patternExpression, sigma);
        desqDfsDriver.runDesqDfs(pruneIrrelevantInputs, useTwoPass);
    }
}
