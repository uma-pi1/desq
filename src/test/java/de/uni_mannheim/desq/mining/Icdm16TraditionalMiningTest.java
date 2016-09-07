package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URL;
import java.util.*;

/**
 * Created by rgemulla on 18.07.2016.
 */
public class Icdm16TraditionalMiningTest extends TraditionalMiningTest {
    @Parameterized.Parameters(name = "Icdm16TraditionalMiningTest-{4}-{0}-{1}-{2}-{3}-")
    public static Collection<Object[]> data() {
        List<Object[]> parameters = new ArrayList<>();
        for (Long sigma : new Long[] {1L,3L,5L,7L})
            for (Integer gamma : new Integer[] {0,1,2})
                for (Integer lambda : new Integer[] {1,3,5,7})
                    for (Boolean generalize : new Boolean[]{ false, true})
                        for (Pair<String, Configuration> miner : MinerConfigurations.all(sigma, gamma, lambda, generalize)) {
                            parameters.add(new Object[] {sigma, gamma, lambda, generalize, miner.getLeft(), miner.getRight()});
                        }
        Collections.sort(parameters, (p1,p2) -> ((String)p1[4]).compareTo((String)p2[4]));
        return parameters;
    }

    public Icdm16TraditionalMiningTest(long sigma, int gamma, int lambda, boolean generalize,
                                       String minerName, Configuration conf) {
        super(sigma, gamma, lambda, generalize, minerName, conf);
    }

    @Override
    public Dictionary getDictionary() throws IOException {
        URL dictFile = TraditionalMiningTest.class.getResource("/icdm16-example/dict.del");
        return DictionaryIO.loadFromDel(dictFile.openStream(), false);
    }

    @Override
    public SequenceReader getSequenceReader() throws IOException {
        URL dataFile = TraditionalMiningTest.class.getResource("/icdm16-example/data.del");
        return new DelSequenceReader(dataFile.openStream(), false);
    }

    @Override
    public boolean computeStatisticsAndFids() {
        return true;
    }

    @Override
    public String getGoldFileBaseName() {
        return "icdm16/icdm16-traditional-patterns-ids";
    }

    @Override
    public String getTestDirectoryName() {
        return getClass().getSimpleName();
    }
}
