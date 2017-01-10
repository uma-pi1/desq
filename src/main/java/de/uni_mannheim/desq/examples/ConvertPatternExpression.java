package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.patex.PatExUtils;

import java.io.IOException;
import java.net.URL;

/**
 * Created by rgemulla on 10.01.2017.
 */
public class ConvertPatternExpression {

    public static final void main(String[] args) throws IOException {
        URL dictFile = ExampleUtils.class.getResource("/icdm16-example/dict.json");
        URL dataFile = ExampleUtils.class.getResource("/icdm16-example/data.del");

        // load the dictionary
        Dictionary dict = Dictionary.loadFrom(dictFile);

        // update hierarchy
        SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
        dict.incFreqs(dataReader);
        dict.recomputeFids();
        System.out.println("Dictionary with statitics:");
        dict.writeJson(System.out);
        System.out.println();

        String patternExpression = "[c|d a1] ([A^ | B=^]+) e";
        System.out.println(patternExpression);

        String patternExpressionSid = PatExUtils.toSidPatEx(dict, patternExpression);
        String patternExpressionGid = PatExUtils.toGidPatEx(dict, patternExpression);
        String patternExpressionFid = PatExUtils.toFidPatEx(dict, patternExpression);

        System.out.println(patternExpressionGid);
        System.out.println(PatExUtils.toGidPatEx(dict, patternExpressionSid));
        System.out.println(PatExUtils.toGidPatEx(dict, patternExpressionGid));
        System.out.println(PatExUtils.toGidPatEx(dict, patternExpressionFid));

        System.out.println(patternExpressionFid);
        System.out.println(PatExUtils.toFidPatEx(dict, patternExpressionSid));
        System.out.println(PatExUtils.toFidPatEx(dict, patternExpressionGid));
        System.out.println(PatExUtils.toFidPatEx(dict, patternExpressionFid));

        System.out.println(patternExpressionSid);
        System.out.println(PatExUtils.toSidPatEx(dict, patternExpressionSid));
        System.out.println(PatExUtils.toSidPatEx(dict, patternExpressionGid));
        System.out.println(PatExUtils.toSidPatEx(dict, patternExpressionFid));

    }
}
