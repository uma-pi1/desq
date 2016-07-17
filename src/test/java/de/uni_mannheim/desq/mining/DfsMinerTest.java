package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.DelPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rgemulla on 16.7.2016.
 */
public class DfsMinerTest {
    @Test
    public void icdm() throws IOException {
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        System.out.println("Temporary folder: " + folder.getRoot());

        int sigma = 2;
        int gamma = 0;
        int lambda = 3;
        boolean generalize = true;
        String fileName;

        sigma = 2; gamma = 0; lambda = 3; generalize = true;
        fileName = "icdm-" + sigma + "-" + gamma + "-" + lambda + "-" + generalize + ".out";
        File actualFile = folder.newFile(fileName);
        icdmTestRun(actualFile, sigma, gamma, lambda, generalize);
        File expectedFile = new File("src/test/java/de/uni_mannheim/desq/mining/DfsMinerTest-gold/" + fileName);
        assertThat(actualFile).hasSameContentAs(expectedFile);
    }

    private void icdmTestRun(File outputDelFile,
                             int sigma, int gamma, int lambda, boolean generalize) throws IOException {
        // load the dictionary
        Dictionary dict = DictionaryIO.loadFromDel(
                new FileInputStream("data/icdm16/example-dict.del"), false);

        // update hierarchy
        SequenceReader dataReader = new DelSequenceReader(
                new FileInputStream("data/icdm16/example-data.del"), false);
        dict.incCounts(dataReader);
        dict.recomputeFids();

        // Perform pattern mining
        dataReader = new DelSequenceReader(
                new FileInputStream("data/icdm16/example-data.del"), false);
        dataReader.setDictionary(dict);
        DesqMinerContext ctx = new DesqMinerContext();
        ctx.sigma = sigma;
        DelPatternWriter result = new DelPatternWriter(new FileOutputStream(outputDelFile), true);
        result.setDictionary(dict);
        ctx.patternWriter = result;
        ctx.dict = dict;
        DesqMiner miner = new DfsMiner(ctx, gamma, lambda, generalize);
        miner.addInputSequences(dataReader);
        miner.mine();
    }
}
