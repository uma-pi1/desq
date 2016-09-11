package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import old.de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by rgemulla on 18.07.2016.
 */
public class Icdm16DesqMiningTest extends DesqMiningTest {
    @Parameterized.Parameters(name = "Icdm16DesqMiningTest-{2}-{0}-{1}")
    public static Collection<Object[]> data() {
        List<Object[]> parameters = new ArrayList<>();
        for (Long sigma : new Long[] {1L,2L,3L})
            for (String patternExpression : new String[] { "[c|d]([A^|B=^]+)e" })
                for (Pair<String, Configuration> miner : MinerConfigurations.all(sigma, patternExpression)) {
                parameters.add(new Object[] {sigma, patternExpression, miner.getLeft(), miner.getRight()});
            }
        Collections.sort(parameters, (p1,p2) -> ((String)p1[2]).compareTo((String)p2[2]));
        return parameters;
    }

    public Icdm16DesqMiningTest(long sigma, String patternExpression,
                                String minerName, Configuration conf) {
        super(sigma, patternExpression, minerName, conf);
    }

    @Override
    public Dictionary getDictionary() throws IOException {
        URL dictFile = TraditionalMiningTest.class.getResource("/icdm16-example/dict.json");
        return Dictionary.loadFrom(dictFile);
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
        return "icdm16/icdm16-desq-patterns-ids";
    }

    @Override
    public String getTestDirectoryName() {
        return getClass().getSimpleName();
    }
}
