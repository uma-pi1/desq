package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.driver.DesqDfsDriver;

import java.io.IOException;

/**
 * Created by ivo on 21.03.17.
 */
public class DesqRunner {

    public static void NYT2005() {
        String inputFile = "data-local/processed/nyt_2005/gid.del";
        String dictFile = "data-local/processed/nyt_2005/dict.avro.gz";
        String outDir = "data-local/processed/run2005";
        String patternExpression = "ENTITY (VB+ NN+ ?IN?) ENTITY";
        int sigma = 5;
        boolean pruneIrrelevantInputs = true;
        boolean useTwoPass = true;
        runDesqDFS(inputFile, dictFile, outDir, patternExpression, sigma, pruneIrrelevantInputs, useTwoPass);
    }

    public static void NYT2006() {
        String inputFile = "data-local/processed/nyt_2006/gid.del";
        String dictFile = "data-local/processed/nyt_2006/dict.avro.gz";
        String outDir = "data-local/processed/run2006";
        String patternExpression = "ENTITY (VB+ NN+ ?IN?) ENTITY";
        int sigma = 5;
        boolean pruneIrrelevantInputs = true;
        boolean useTwoPass = true;
        runDesqDFS(inputFile, dictFile, outDir, patternExpression, sigma, pruneIrrelevantInputs, useTwoPass);
    }

    public static void runDesqDFS(String iF, String dF, String oD, String pE, int s, boolean pII, boolean uTP) {
        DesqDfsDriver driver = new DesqDfsDriver(iF, dF, oD, pE, s);
        try {
            driver.runDesqDfs(pII, uTP);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        NYT2005();
        NYT2006();
    }
}
