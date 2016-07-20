package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URL;
import java.util.*;

/**
 * Created by rgemulla on 18.07.2016.
 */
public abstract class Icdm16TraditionalMiningTest extends TraditionalMiningTest {
    @Parameterized.Parameters(name = "icdm16-traditional-patterns-ids-{0}-{1}-{2}-{3}")
    public static Collection<Object[]> data() {
        List<Object[]> parameters = new ArrayList<>();
        for (Long sigma : new Long[] {1L,3L,5L,7L})
            for (Integer gamma : new Integer[] {0,1,2})
                for (Integer lambda : new Integer[] {1,3,5,7})
                    for (Boolean generalize : new Boolean[]{ false, true})
                        parameters.add(new Object[] {sigma, gamma, lambda, generalize});
        return parameters;
    }

    public Icdm16TraditionalMiningTest(long sigma, int gamma, int lambda, boolean generalize) {
        super(sigma, gamma, lambda, generalize);
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
    public String getBaseFileName() {
        return "icdm16/icdm16-traditional-patterns-ids";
    }
}
